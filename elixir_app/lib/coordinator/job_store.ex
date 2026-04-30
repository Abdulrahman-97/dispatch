defmodule Dispatch.Coordinator.JobStore do
  @moduledoc false

  @processing_key "jobs:processing"
  @queue_key "jobs:queue"
  @redis_name Dispatch.Redis
  @mark_running_script """
  local key = KEYS[1]
  local status = redis.call("HGET", key, "status")

  if not status then
    return "not_found"
  end

  if status ~= "queued" then
    return "invalid_transition:" .. status
  end

  redis.call(
    "HSET",
    key,
    "status",
    "running",
    "started_at",
    ARGV[1],
    "finished_at",
    "",
    "result",
    "",
    "error",
    "",
    "worker_name",
    ARGV[2]
  )

  return "ok"
  """
  @complete_script """
  local key = KEYS[1]
  local processing_key = KEYS[2]
  local status = redis.call("HGET", key, "status")

  if not status then
    return "not_found"
  end

  if status ~= "running" then
    return "invalid_transition:" .. status
  end

  if redis.call("HGET", key, "started_at") ~= ARGV[2] then
    return "stale_attempt"
  end

  redis.call(
    "HSET",
    key,
    "status",
    ARGV[3],
    "result",
    ARGV[4],
    "error",
    ARGV[5],
    "finished_at",
    ARGV[6]
  )

  if ARGV[7] ~= "" then
    redis.call("HSET", key, "worker_name", ARGV[7])
  end

  redis.call("LREM", processing_key, "1", ARGV[1])

  return "ok"
  """
  @requeue_stuck_script """
  local key = KEYS[1]
  local processing_key = KEYS[2]
  local queue_key = KEYS[3]
  local status = redis.call("HGET", key, "status")

  if not status then
    return "not_found"
  end

  if status ~= "running" then
    return "invalid_transition:" .. status
  end

  if redis.call("HGET", key, "started_at") ~= ARGV[2] then
    return "stale_attempt"
  end

  redis.call("LREM", processing_key, "1", ARGV[1])
  redis.call("LPUSH", queue_key, ARGV[1])
  redis.call(
    "HSET",
    key,
    "status",
    "queued",
    "started_at",
    "",
    "finished_at",
    "",
    "result",
    "",
    "error",
    "",
    "worker_name",
    ""
  )

  return "ok"
  """

  def put_new(job_id, payload_json) do
    Redix.command(@redis_name, [
      "HSET",
      job_key(job_id),
      "status",
      "queued",
      "payload",
      payload_json,
      "result",
      "",
      "error",
      "",
      "inserted_at",
      now_iso8601(),
      "started_at",
      "",
      "finished_at",
      "",
      "worker_name",
      ""
    ])
  end

  def get(job_id) do
    case Redix.command(@redis_name, ["HGETALL", job_key(job_id)]) do
      {:ok, []} -> {:error, :not_found}
      {:ok, fields} -> {:ok, fields_to_map(fields)}
      {:error, reason} -> {:error, reason}
    end
  end

  def payload(job_id) do
    with {:ok, job} <- get(job_id),
         payload when is_binary(payload) <- Map.get(job, "payload"),
         {:ok, decoded} <- Jason.decode(payload) do
      {:ok, decoded}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_payload}
    end
  end

  def mark_running(job_id, started_at, worker_name \\ nil) do
    case transition(@mark_running_script, [job_key(job_id)], [
           started_at,
           normalize_worker_name(worker_name)
         ]) do
      {:ok, "ok"} -> {:ok, :running}
      {:ok, "not_found"} -> {:error, :not_found}
      {:ok, <<"invalid_transition:", _::binary>>} -> {:error, :invalid_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  def complete(job_id, started_at, attrs) do
    case transition(
           @complete_script,
           [job_key(job_id), @processing_key],
           [
             job_id,
             started_at,
             attrs["status"],
             attrs["result"] || "",
             attrs["error"] || "",
             now_iso8601(),
             normalize_worker_name(attrs["worker_name"])
           ]
         ) do
      {:ok, "ok"} -> {:ok, :completed}
      {:ok, "not_found"} -> {:error, :not_found}
      {:ok, "stale_attempt"} -> {:error, :stale_attempt}
      {:ok, <<"invalid_transition:", _::binary>>} -> {:error, :invalid_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  def requeue_stuck(job_id, started_at) do
    case transition(
           @requeue_stuck_script,
           [job_key(job_id), @processing_key, @queue_key],
           [job_id, started_at]
         ) do
      {:ok, "ok"} -> {:ok, :requeued}
      {:ok, "not_found"} -> {:error, :not_found}
      {:ok, "stale_attempt"} -> {:error, :stale_attempt}
      {:ok, <<"invalid_transition:", _::binary>>} -> {:error, :invalid_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  def format_status(job_id, fields) do
    %{
      job_id: job_id,
      status: fields["status"],
      result: normalize_field(fields["result"]),
      error: normalize_field(fields["error"]),
      worker_name: normalize_field(fields["worker_name"])
    }
  end

  def processing_started_at(fields) do
    normalize_field(fields["started_at"])
  end

  defp fields_to_map(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {key, value} end)
  end

  defp normalize_field(""), do: nil
  defp normalize_field(value), do: value

  defp normalize_worker_name(value) when is_binary(value), do: String.trim(value)
  defp normalize_worker_name(_value), do: ""

  defp transition(script, keys, args) do
    Redix.command(@redis_name, ["EVAL", script, Integer.to_string(length(keys)) | keys ++ args])
  end

  defp job_key(job_id), do: "job:#{job_id}"

  defp now_iso8601 do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
