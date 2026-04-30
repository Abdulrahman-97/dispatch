defmodule Dispatch.Coordinator.RateLimiter do
  @moduledoc false

  alias Dispatch.RateLimit

  @env_var "DISPATCH_RATE_LIMITS_JSON"
  @redis_name Dispatch.Redis
  @expire_grace_seconds 5

  @acquire_script """
  local current = tonumber(redis.call("GET", KEYS[1]) or "0")
  local limit = tonumber(ARGV[1])
  local cost = tonumber(ARGV[2])
  local ttl = tonumber(ARGV[3])

  if current + cost > limit then
    return {0, current}
  end

  local next_value = redis.call("INCRBY", KEYS[1], cost)
  redis.call("EXPIRE", KEYS[1], ttl)

  return {1, next_value}
  """

  def validate_config! do
    case configured_limits() do
      {:ok, _limits} ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "invalid #{@env_var}: #{reason}"
    end
  end

  def acquire(rate_limit_key, rate_limit_cost, opts \\ []) do
    with {:ok, key} <- RateLimit.normalize_key(rate_limit_key),
         {:ok, cost} <- RateLimit.normalize_cost(rate_limit_cost),
         {:ok, limits} <- limits_from_opts(opts),
         {:ok, limit_config} <- fetch_limit_config(limits, key) do
      now_seconds = Keyword.get_lazy(opts, :now_seconds, fn -> System.system_time(:second) end)
      redis_key = window_key(key, limit_config.window_seconds, now_seconds)
      command = Keyword.get(opts, :command, &redis_command/1)

      case command.([
             "EVAL",
             @acquire_script,
             "1",
             redis_key,
             Integer.to_string(limit_config.limit),
             Integer.to_string(cost),
             Integer.to_string(limit_config.window_seconds + @expire_grace_seconds)
           ]) do
        {:ok, [1 | _rest]} ->
          {:ok,
           %{
             allowed: true,
             retry_interval_ms: limit_config.retry_interval_ms,
             redis_key: redis_key
           }}

        {:ok, [0 | _rest]} ->
          {:ok,
           %{
             allowed: false,
             retry_interval_ms: limit_config.retry_interval_ms,
             redis_key: redis_key
           }}

        {:ok, other} ->
          {:error, "unexpected redis rate limit response: #{inspect(other)}"}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp limits_from_opts(opts) do
    case Keyword.fetch(opts, :limits) do
      {:ok, limits} -> {:ok, limits}
      :error -> configured_limits()
    end
  end

  def configured_limits do
    case System.get_env(@env_var) do
      nil -> {:ok, %{}}
      "" -> {:ok, %{}}
      raw -> parse_limits(raw)
    end
  end

  def parse_limits(raw) do
    with {:ok, decoded} <- Jason.decode(raw),
         true <- is_map(decoded) do
      parse_limit_map(decoded)
    else
      {:error, %Jason.DecodeError{}} -> {:error, "must be valid JSON"}
      false -> {:error, "must be a JSON object"}
    end
  end

  def window_key(rate_limit_key, window_seconds, now_seconds) do
    window_start = div(now_seconds, window_seconds) * window_seconds
    "rate_limit:#{rate_limit_key}:#{window_start}"
  end

  defp parse_limit_map(decoded) do
    Enum.reduce_while(decoded, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      with {:ok, normalized_key} <- RateLimit.normalize_key(key),
           {:ok, config} <- parse_limit_config(value) do
        {:cont, {:ok, Map.put(acc, normalized_key, config)}}
      else
        {:error, reason} -> {:halt, {:error, "#{key}: #{reason}"}}
      end
    end)
  end

  defp parse_limit_config(value) when is_map(value) do
    with {:ok, limit} <- required_positive_integer(value, "limit"),
         {:ok, window_seconds} <- optional_positive_integer(value, "window_seconds", 60),
         {:ok, retry_interval_ms} <- optional_positive_integer(value, "retry_interval_ms", 1_000) do
      {:ok,
       %{
         limit: limit,
         window_seconds: window_seconds,
         retry_interval_ms: retry_interval_ms
       }}
    end
  end

  defp parse_limit_config(_value), do: {:error, "config must be an object"}

  defp required_positive_integer(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> positive_integer(value, key)
      :error -> {:error, "#{key} is required"}
    end
  end

  defp optional_positive_integer(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} -> positive_integer(value, key)
      :error -> {:ok, default}
    end
  end

  defp positive_integer(value, _key) when is_integer(value) and value > 0, do: {:ok, value}

  defp positive_integer(value, key) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _ -> {:error, "#{key} must be a positive integer"}
    end
  end

  defp positive_integer(_value, key), do: {:error, "#{key} must be a positive integer"}

  defp fetch_limit_config(limits, key) do
    case Map.fetch(limits, key) do
      {:ok, config} -> {:ok, config}
      :error -> {:error, "rate limit is not configured: #{key}"}
    end
  end

  defp redis_command(command), do: Redix.command(@redis_name, command)
end
