defmodule Dispatch.Coordinator.Recovery do
  @moduledoc false

  use GenServer

  require Logger

  alias Dispatch.Coordinator.JobQueue
  alias Dispatch.Coordinator.JobStore
  alias Dispatch.Observability

  @scan_interval_ms 30_000
  @default_stuck_after_seconds 1_800

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
         heartbeat_at when is_binary(heartbeat_at) <- JobStore.processing_heartbeat_at(job),
         true <- older_than_threshold?(heartbeat_at, now, stuck_after_seconds()) do
      recover_job(job_id, job, started_at, heartbeat_at)
    else
      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        Logger.error("recovery_lookup_failed job=#{job_id} reason=#{inspect(reason)}")

      _ ->
        :ok
    end
  end

  def recovery_action(%{"job_type" => "dagster_run"}), do: :mark_worker_lost
  def recovery_action(_job), do: :requeue

  defp recover_job(job_id, job, started_at, heartbeat_at) do
    result =
      case recovery_action(job) do
        :mark_worker_lost ->
          JobStore.mark_worker_lost(job_id, started_at)

        :requeue ->
          JobStore.requeue_stuck(job_id, started_at)
      end

    case result do
      {:ok, :worker_lost} ->
        Observability.event(
          "worker_lost",
          %{
            dispatch_job_id: job_id,
            dagster_run_id: job["dagster_run_id"],
            worker_name: job["worker_name"],
            worker_instance_id: job["worker_instance_id"],
            started_at: started_at,
            heartbeat_at: heartbeat_at,
            recovery_action: "reconciliation_required"
          },
          :warning
        )

      {:ok, :requeued} ->
        Observability.event(
          "worker_lost",
          %{
            dispatch_job_id: job_id,
            worker_name: job["worker_name"],
            worker_instance_id: job["worker_instance_id"],
            started_at: started_at,
            heartbeat_at: heartbeat_at,
            recovery_action: "requeued"
          },
          :warning
        )

      {:error, :stale_attempt} ->
        :ok

      {:error, :invalid_transition} ->
        :ok

      {:error, reason} ->
        Observability.event(
          "worker_loss_recovery_failed",
          %{dispatch_job_id: job_id, reason: inspect(reason)},
          :error
        )
    end
  end

  def stuck_after_seconds do
    "DISPATCH_JOB_STUCK_AFTER_SECONDS"
    |> System.get_env(
      System.get_env("JOB_STUCK_AFTER_SECONDS", "#{@default_stuck_after_seconds}")
    )
    |> String.to_integer()
  end

  def older_than_threshold?(started_at, now, threshold_seconds \\ stuck_after_seconds()) do
    case DateTime.from_iso8601(started_at) do
      {:ok, started_at_dt, _offset} ->
        DateTime.diff(now, started_at_dt, :second) > threshold_seconds

      {:error, _reason} ->
        false
    end
  end

  defp schedule_scan do
    Process.send_after(self(), :scan, @scan_interval_ms)
  end
end
