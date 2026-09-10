defmodule Dispatch.Application do
  @moduledoc false

  use Application

  @redis_name Dispatch.Redis

  @impl true
  def start(_type, _args) do
    Dispatch.Coordinator.RateLimiter.validate_config!()

    children =
      case role() do
        "coordinator" ->
          [redis_child(), recovery_child(), coordinator_child()]

        "worker" ->
          [{Task.Supervisor, name: Dispatch.Worker.Tasks}, {Dispatch.Worker.Scheduler, []}]

        _ ->
          []
      end

    # Losing scheduler state must not silently restore capacity beside old tasks.
    # A worker supervision failure stops the application; coordinator attempts
    # remain unresolved. Coordinator supervision retains its existing policy.
    options =
      if role() == "worker",
        do: [strategy: :one_for_all, max_restarts: 0, name: Dispatch.Supervisor],
        else: [strategy: :one_for_one, name: Dispatch.Supervisor]

    Supervisor.start_link(children, options)
  end

  # OTP calls prep_stop before stopping the application's supervision tree.
  # Scheduler and executor tasks remain alive, including their heartbeats.
  @impl true
  def prep_stop(state) do
    if role() == "worker" do
      if Process.whereis(Dispatch.Worker.Scheduler) do
        Dispatch.Worker.Scheduler.drain()
      else
        Dispatch.Observability.event(
          "worker_drain_unavailable",
          %{
            outcome: "scheduler ownership lost; coordinator reconciliation required"
          },
          :error
        )
      end
    end

    state
  end

  defp coordinator_child do
    {Bandit, plug: Dispatch.Coordinator.Router, scheme: :http, port: coordinator_port()}
  end

  defp recovery_child do
    {Dispatch.Coordinator.Recovery, []}
  end

  defp redis_child do
    {Redix, {redis_url(), [name: @redis_name, sync_connect: false]}}
  end

  defp role do
    System.get_env("APP_ROLE") ||
      if System.get_env("MIX_ENV") == "test", do: "none", else: "coordinator"
  end

  defp redis_url do
    System.get_env("REDIS_URL", "redis://localhost:6379/0")
  end

  defp coordinator_port do
    System.get_env("PORT", "4000")
    |> String.to_integer()
  end
end
