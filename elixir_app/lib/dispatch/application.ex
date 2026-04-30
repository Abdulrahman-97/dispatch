defmodule Dispatch.Application do
  @moduledoc false

  use Application

  @redis_name Dispatch.Redis

  @impl true
  def start(_type, _args) do
    children =
      case role() do
        "coordinator" ->
          [redis_child(), recovery_child(), coordinator_child()]

        "worker" ->
          worker_children()

        _ ->
          []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: Dispatch.Supervisor)
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

  defp worker_children do
    1..worker_concurrency()
    |> Enum.map(fn slot ->
      Supervisor.child_spec({Dispatch.Worker.Poller, [slot: slot]},
        id: {Dispatch.Worker.Poller, slot}
      )
    end)
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

  defp worker_concurrency do
    System.get_env("WORKER_CONCURRENCY", "5")
    |> String.to_integer()
  end
end
