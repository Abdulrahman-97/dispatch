defmodule Dispatch.Coordinator.JobStore do
  @moduledoc false

  @processing_key "jobs:processing"
  @queue_key "jobs:queue"
  @redis_name Dispatch.Redis
  @claim_queued_script """
  local job_key = KEYS[1]
  local queue_key = KEYS[2]
  local processing_key = KEYS[3]
  local job_id = ARGV[1]
  local started_at = ARGV[2]
  local worker_name = ARGV[3]
  local worker_resources = ARGV[4]
  local rate_limit_count = tonumber(ARGV[5])
  local arg_index = 6

  local status = redis.call("HGET", job_key, "status")

  if not status then
    return {"not_found"}
  end

  if status ~= "queued" then
    return {"invalid_transition", status}
  end

  if not redis.call("LPOS", queue_key, job_id) then
    return {"not_queued"}
  end

  for i = 1, rate_limit_count do
    local redis_key = KEYS[3 + i]
    local limit = tonumber(ARGV[arg_index])
    local cost = tonumber(ARGV[arg_index + 1])
    local current = tonumber(redis.call("GET", redis_key) or "0")

    if current + cost > limit then
      return {"rate_limited", tostring(i), tostring(current)}
    end

    arg_index = arg_index + 3
  end

  arg_index = 6

  for i = 1, rate_limit_count do
    local redis_key = KEYS[3 + i]
    local cost = tonumber(ARGV[arg_index + 1])
    local ttl = tonumber(ARGV[arg_index + 2])

    redis.call("INCRBY", redis_key, cost)
    redis.call("EXPIRE", redis_key, ttl)

    arg_index = arg_index + 3
  end

  redis.call("LREM", queue_key, "1", job_id)
  redis.call("LPUSH", processing_key, job_id)
  redis.call(
    "HSET",
    job_key,
    "status",
    "running",
    "started_at",
    started_at,
    "finished_at",
    "",
    "result",
    "",
    "error",
    "",
    "worker_name",
    worker_name,
    "worker_resources",
    worker_resources,
    "queued_reason",
    ""
  )

  return {"ok"}
  """
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

  if ARGV[8] ~= "" then
    redis.call("HSET", key, "rate_limit_wait_ms", ARGV[8])
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
    "",
    "worker_resources",
    "",
    "rate_limit_wait_ms",
    "0",
    "queued_reason",
    "recovered_stuck_job"
  )

  return "ok"
  """
  @set_queue_diagnostic_script """
  local key = KEYS[1]
  local status = redis.call("HGET", key, "status")

  if status == "queued" then
    redis.call("HSET", key, "queued_reason", ARGV[1])
    return "ok"
  end

  return "skipped"
  """
  @increment_rate_limit_wait_script """
  local key = KEYS[1]
  local status = redis.call("HGET", key, "status")

  if status == "queued" then
    redis.call("HINCRBY", key, "rate_limit_wait_ms", ARGV[1])
    redis.call("HSET", key, "queued_reason", ARGV[2])
    return "ok"
  end

  return "skipped"
  """

  def put_new(job_id, payload_json, attrs \\ %{}) do
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
      "",
      "rate_limit_key",
      attrs["rate_limit_key"] || "",
      "rate_limit_cost",
      attrs["rate_limit_cost"] || "",
      "rate_limit_wait_ms",
      attrs["rate_limit_wait_ms"] || "",
      "resources",
      attrs["resources"] || "",
      "rate_limits",
      attrs["rate_limits"] || "",
      "worker_resources",
      "",
      "queued_reason",
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

  def claim_queued(job_id, started_at, worker_name, worker_resources, rate_limit_entries) do
    keys =
      [job_key(job_id), @queue_key, @processing_key] ++
        Enum.map(rate_limit_entries, & &1.redis_key)

    args =
      [
        job_id,
        started_at,
        normalize_worker_name(worker_name),
        Jason.encode!(worker_resources),
        Integer.to_string(length(rate_limit_entries))
      ] ++
        Enum.flat_map(rate_limit_entries, fn entry ->
          [
            Integer.to_string(entry.limit),
            Integer.to_string(entry.cost),
            Integer.to_string(entry.ttl)
          ]
        end)

    case transition(@claim_queued_script, keys, args) do
      {:ok, ["ok"]} ->
        {:ok, :running}

      {:ok, ["rate_limited", index | _rest]} ->
        entry = Enum.at(rate_limit_entries, String.to_integer(index) - 1)
        {:error, {:rate_limited, entry}}

      {:ok, ["not_found"]} ->
        {:error, :not_found}

      {:ok, ["not_queued"]} ->
        {:error, :not_queued}

      {:ok, ["invalid_transition" | _rest]} ->
        {:error, :invalid_transition}

      {:error, reason} ->
        {:error, reason}
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
             normalize_worker_name(attrs["worker_name"]),
             normalize_non_negative_integer(attrs["rate_limit_wait_ms"])
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

  def set_queue_diagnostic(job_id, reason) do
    transition(@set_queue_diagnostic_script, [job_key(job_id)], [reason])
  end

  def increment_rate_limit_wait(job_id, wait_ms, reason) do
    transition(@increment_rate_limit_wait_script, [job_key(job_id)], [
      Integer.to_string(wait_ms),
      reason
    ])
  end

  def format_status(job_id, fields) do
    %{
      job_id: job_id,
      status: fields["status"],
      result: normalize_field(fields["result"]),
      error: normalize_field(fields["error"]),
      worker_name: normalize_field(fields["worker_name"]),
      rate_limit_key: normalize_field(fields["rate_limit_key"]),
      rate_limit_cost: normalize_integer_field(fields["rate_limit_cost"]),
      rate_limits: decode_json_field(fields["rate_limits"]),
      rate_limit_wait_ms: normalize_integer_field(fields["rate_limit_wait_ms"]),
      resources: decode_json_field(fields["resources"]),
      worker_resources: decode_json_field(fields["worker_resources"]),
      queued_reason: normalize_field(fields["queued_reason"]),
      queue_wait_ms: queue_wait_ms(fields["inserted_at"], fields["started_at"])
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

  defp normalize_non_negative_integer(value) when is_integer(value) and value >= 0 do
    Integer.to_string(value)
  end

  defp normalize_non_negative_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer >= 0 -> Integer.to_string(integer)
      _ -> ""
    end
  end

  defp normalize_non_negative_integer(_value), do: ""

  defp normalize_integer_field(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> nil
    end
  end

  defp normalize_integer_field(_value), do: nil

  defp decode_json_field(value) when is_binary(value) and value != "" do
    case Jason.decode(value) do
      {:ok, decoded} -> decoded
      _ -> nil
    end
  end

  defp decode_json_field(_value), do: nil

  defp queue_wait_ms(inserted_at, started_at)
       when is_binary(inserted_at) and is_binary(started_at) do
    with {:ok, inserted, _offset} <- DateTime.from_iso8601(inserted_at),
         {:ok, started, _offset} <- DateTime.from_iso8601(started_at) do
      DateTime.diff(started, inserted, :millisecond)
    else
      _ -> nil
    end
  end

  defp queue_wait_ms(_inserted_at, _started_at), do: nil

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
