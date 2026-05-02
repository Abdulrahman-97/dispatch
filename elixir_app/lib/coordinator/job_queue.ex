defmodule Dispatch.Coordinator.JobQueue do
  @moduledoc false

  require Logger

  alias Dispatch.Coordinator.RateLimiter
  alias Dispatch.Coordinator.JobStore
  alias Dispatch.RateLimit
  alias Dispatch.Resources
  alias Dispatch.UUID

  @queue_key "jobs:queue"
  @processing_key "jobs:processing"
  @redis_name Dispatch.Redis

  def enqueue(job_type, params, attrs \\ %{}) do
    job_id = UUID.generate()
    payload_json = Jason.encode!(%{job_type: job_type, params: params})
    group_id = attrs["group_id"] || ""

    with {:ok, resources} <- Resources.requirements_from_params(params),
         {:ok, rate_limit_metadata} <- RateLimit.metadata_from_params(params),
         attrs =
           Map.merge(rate_limit_metadata, %{
             "resources" => Resources.encode(resources),
             "group_id" => group_id
           }),
         {:ok, _} <- JobStore.put_new(job_id, payload_json, attrs),
         {:ok, _} <- maybe_add_group_job(group_id, job_id),
         {:ok, _} <- Redix.command(@redis_name, ["RPUSH", @queue_key, job_id]) do
      {:ok, job_id}
    end
  end

  def claim_next(
        worker_name \\ nil,
        available_resources \\ %{"default_slots" => 1},
        worker_resources \\ nil
      ) do
    worker_resources = worker_resources || available_resources

    with {:ok, normalized_available} <-
           Resources.normalize_available_resource_map(available_resources),
         {:ok, normalized_worker_resources} <- Resources.normalize_resource_map(worker_resources) do
      case Redix.command(@redis_name, ["LRANGE", @queue_key, "0", "-1"]) do
        {:ok, []} ->
          :empty

        {:ok, job_ids} ->
          claim_first_matching(
            job_ids,
            worker_name,
            normalized_available,
            normalized_worker_resources
          )

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def processing_job_ids do
    Redix.command(@redis_name, ["LRANGE", @processing_key, "0", "-1"])
  end

  def remove_from_processing(job_id) do
    Redix.command(@redis_name, ["LREM", @processing_key, "1", job_id])
  end

  defp claim_first_matching(job_ids, worker_name, available_resources, worker_resources) do
    job_ids
    |> Enum.reverse()
    |> Enum.reduce_while(:empty, fn job_id, _acc ->
      case maybe_claim_job(job_id, worker_name, available_resources, worker_resources) do
        :skip -> {:cont, :empty}
        :empty -> {:cont, :empty}
        {:ok, job} -> {:halt, {:ok, job}}
        {:error, :not_queued} -> {:cont, :empty}
        {:error, :invalid_transition} -> {:cont, :empty}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp maybe_claim_job(job_id, worker_name, available_resources, worker_resources) do
    with {:ok, payload} <- JobStore.payload(job_id),
         params when is_map(params) <- Map.get(payload, "params", %{}),
         {:ok, requirements} <- Resources.requirements_from_params(params),
         :ok <- resource_fit(job_id, requirements, available_resources),
         {:ok, rate_specs} <- RateLimit.specs_from_params(params),
         {:ok, rate_entries} <- RateLimiter.entries_for_specs(rate_specs) do
      started_at = now_iso8601()

      case JobStore.claim_queued(job_id, started_at, worker_name, worker_resources, rate_entries) do
        {:ok, :running} ->
          claimed_job(job_id, started_at, worker_name, requirements)

        {:error, {:rate_limited, entry}} ->
          reason = "waiting_on_rate_limit:#{entry.key}"
          _ = JobStore.increment_rate_limit_wait(job_id, entry.retry_interval_ms, reason)

          Logger.warning(
            "worker=#{worker_name} job=#{job_id} rate_limit_wait key=#{entry.key} cost=#{entry.cost} retry_interval_ms=#{entry.retry_interval_ms}"
          )

          :skip

        {:error, {:group_limited, group_id}} ->
          _ = JobStore.set_queue_diagnostic(job_id, "group_concurrency_limit:#{group_id}")
          :skip

        other ->
          other
      end
    else
      {:error, :skip} ->
        :skip

      {:error, reason} when is_binary(reason) ->
        _ = JobStore.set_queue_diagnostic(job_id, reason)
        :skip

      _ ->
        _ = JobStore.set_queue_diagnostic(job_id, "invalid_job_payload")
        :skip
    end
  end

  defp resource_fit(job_id, requirements, available_resources) do
    missing_keys = Resources.missing_keys(requirements, available_resources)

    cond do
      missing_keys != [] ->
        reason = "no_worker_with_required_resource_keys:#{Enum.join(missing_keys, ",")}"
        _ = JobStore.set_queue_diagnostic(job_id, reason)
        {:error, :skip}

      not Resources.fits?(requirements, available_resources) ->
        _ = JobStore.set_queue_diagnostic(job_id, "insufficient_available_capacity")
        {:error, :skip}

      true ->
        :ok
    end
  end

  defp claimed_job(job_id, started_at, worker_name, requirements) do
    with {:ok, payload} <- JobStore.payload(job_id),
         job_type when is_binary(job_type) <- Map.get(payload, "job_type"),
         params when is_map(params) <- Map.get(payload, "params", %{}) do
      {:ok,
       %{
         job_id: job_id,
         job_type: job_type,
         params: params,
         started_at: started_at,
         worker_name: worker_name,
         resources: requirements
       }}
    else
      _ ->
        _ =
          JobStore.complete(job_id, started_at, %{
            "status" => "failed",
            "result" => nil,
            "error" => "invalid job payload"
          })

        {:error, :invalid_job_payload}
    end
  end

  defp maybe_add_group_job("", _job_id), do: {:ok, :ungrouped}
  defp maybe_add_group_job(nil, _job_id), do: {:ok, :ungrouped}
  defp maybe_add_group_job(group_id, job_id), do: JobStore.add_group_job(group_id, job_id)

  defp now_iso8601 do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
