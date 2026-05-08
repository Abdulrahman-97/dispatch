defmodule Dispatch.Coordinator.Idempotency do
  @moduledoc false

  @redis_name Dispatch.Redis

  def reserve(scope, key, job_id, opts \\ []) do
    redis_key = redis_key(scope, key)
    command = Keyword.get(opts, :command, &Redix.command(@redis_name, &1))

    case command.(["SET", redis_key, job_id, "NX"]) do
      {:ok, "OK"} ->
        {:ok, :reserved}

      {:ok, nil} ->
        case command.(["GET", redis_key]) do
          {:ok, existing_job_id} when is_binary(existing_job_id) and existing_job_id != "" ->
            {:ok, {:existing, existing_job_id}}

          {:ok, _value} ->
            {:error, :missing_existing_job_id}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def release(scope, key, job_id, opts \\ []) do
    redis_key = redis_key(scope, key)
    command = Keyword.get(opts, :command, &Redix.command(@redis_name, &1))

    script = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end

    return 0
    """

    command.(["EVAL", script, "1", redis_key, job_id])
  end

  defp redis_key(scope, key), do: "idempotency:#{scope}:#{key}"
end
