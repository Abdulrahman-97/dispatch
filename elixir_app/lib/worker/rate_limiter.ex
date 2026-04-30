defmodule Dispatch.Worker.RateLimiter do
  @moduledoc false

  require Logger

  alias Dispatch.RateLimit

  def acquire(job, opts) do
    case RateLimit.spec_from_params(Map.get(job, "params", %{})) do
      :none ->
        {:ok, 0}

      {:ok, spec} ->
        acquire_loop(job, spec, opts, 0)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire_loop(job, spec, opts, wait_ms) do
    post_json = Keyword.fetch!(opts, :post_json)
    sleep = Keyword.get(opts, :sleep, &:timer.sleep/1)
    worker_name = Keyword.get(opts, :worker_name)

    payload = %{
      "job_id" => job["job_id"],
      "worker_name" => worker_name,
      "rate_limit_key" => spec.key,
      "rate_limit_cost" => spec.cost
    }

    case post_json.("/internal/rate_limit/acquire", payload) do
      {:ok, 200, body} ->
        case Jason.decode(body) do
          {:ok, %{"allowed" => true}} -> {:ok, wait_ms}
          {:ok, response} -> {:error, "unexpected rate limit response: #{inspect(response)}"}
          {:error, reason} -> {:error, "invalid rate limit response: #{inspect(reason)}"}
        end

      {:ok, 429, body} ->
        retry_interval_ms = retry_interval_ms(body)

        Logger.warning(
          "worker=#{worker_name} job=#{job["job_id"]} rate_limit_wait key=#{spec.key} cost=#{spec.cost} retry_interval_ms=#{retry_interval_ms}"
        )

        sleep.(retry_interval_ms)
        acquire_loop(job, spec, opts, wait_ms + retry_interval_ms)

      {:ok, 422, body} ->
        {:error, "rate limit rejected: #{String.trim(to_string(body))}"}

      {:ok, status, body} ->
        {:error, "rate limit acquire failed status=#{status} body=#{inspect(body)}"}

      {:error, reason} ->
        {:error, "rate limit acquire failed reason=#{inspect(reason)}"}
    end
  end

  defp retry_interval_ms(body) do
    case Jason.decode(body) do
      {:ok, %{"retry_interval_ms" => value}} when is_integer(value) and value > 0 -> value
      _ -> 1_000
    end
  end
end
