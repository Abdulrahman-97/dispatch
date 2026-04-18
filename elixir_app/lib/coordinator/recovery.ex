defmodule Dispatch.Coordinator.Recovery do
  @moduledoc false

  use GenServer

  require Logger

  alias Dispatch.Coordinator.JobQueue
  alias Dispatch.Coordinator.JobStore

  @scan_interval_ms 30_000
  @stuck_after_seconds 180

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_scan()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:scan, state) do
    recover_stuck_jobs()
    schedule_scan()
    {:noreply, state}
  end

  defp recover_stuck_jobs do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    case JobQueue.processing_job_ids() do
      {:ok, job_ids} ->
        Enum.each(job_ids, &maybe_recover_job(&1, now))

      {:error, reason} ->
        Logger.error("recovery_scan_failed reason=#{inspect(reason)}")
    end
  end

  defp maybe_recover_job(job_id, now) do
    with {:ok, job} <- JobStore.get(job_id),
         "running" <- job["status"],
         started_at when is_binary(started_at) <- JobStore.processing_started_at(job),
         true <- older_than_threshold?(started_at, now) do
      case JobStore.requeue_stuck(job_id, started_at) do
        {:ok, :requeued} ->
          Logger.warning("recovery_requeued job=#{job_id} started_at=#{started_at}")

        {:error, :stale_attempt} ->
          :ok

        {:error, :invalid_transition} ->
          :ok

        {:error, reason} ->
          Logger.error("recovery_requeue_failed job=#{job_id} reason=#{inspect(reason)}")
      end
    else
      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        Logger.error("recovery_lookup_failed job=#{job_id} reason=#{inspect(reason)}")

      _ ->
        :ok
    end
  end

  defp older_than_threshold?(started_at, now) do
    case DateTime.from_iso8601(started_at) do
      {:ok, started_at_dt, _offset} ->
        DateTime.diff(now, started_at_dt, :second) > @stuck_after_seconds

      {:error, _reason} ->
        false
    end
  end

  defp schedule_scan do
    Process.send_after(self(), :scan, @scan_interval_ms)
  end
end
