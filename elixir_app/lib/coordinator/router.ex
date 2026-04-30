defmodule Dispatch.Coordinator.Router do
  @moduledoc false

  use Plug.Router

  alias Dispatch.Coordinator.JobQueue
  alias Dispatch.Coordinator.RateLimiter
  alias Dispatch.Coordinator.JobStore

  plug(Plug.Logger)

  plug(Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  post "/jobs" do
    job_type = conn.body_params["job_type"]
    params = Map.get(conn.body_params, "params", %{})

    cond do
      not valid_job_type?(job_type) ->
        json(conn, 422, %{error: "job_type must match ^[a-z0-9_]+$"})

      not is_map(params) ->
        json(conn, 422, %{error: "params must be a JSON object"})

      true ->
        case JobQueue.enqueue(job_type, params) do
          {:ok, job_id} -> json(conn, 201, %{job_id: job_id})
          {:error, reason} when is_binary(reason) -> json(conn, 422, %{error: reason})
          {:error, reason} -> redis_error(conn, reason)
        end
    end
  end

  get "/jobs/:job_id" do
    case JobStore.get(job_id) do
      {:ok, job} ->
        json(conn, 200, JobStore.format_status(job_id, job))

      {:error, :not_found} ->
        json(conn, 404, %{error: "job not found"})

      {:error, reason} ->
        redis_error(conn, reason)
    end
  end

  post "/internal/poll" do
    case JobQueue.claim_next(conn.body_params["worker_name"]) do
      {:ok, job} ->
        json(conn, 200, job)

      :empty ->
        send_resp(conn, 204, "")

      {:error, reason} ->
        redis_error(conn, reason)
    end
  end

  post "/internal/rate_limit/acquire" do
    key = conn.body_params["rate_limit_key"]
    cost = Map.get(conn.body_params, "rate_limit_cost", 1)

    case RateLimiter.acquire(key, cost) do
      {:ok, %{allowed: true} = result} ->
        json(conn, 200, %{
          allowed: true,
          retry_interval_ms: result.retry_interval_ms
        })

      {:ok, %{allowed: false} = result} ->
        json(conn, 429, %{
          allowed: false,
          retry_interval_ms: result.retry_interval_ms
        })

      {:error, reason} when is_binary(reason) ->
        json(conn, 422, %{error: reason})

      {:error, reason} ->
        redis_error(conn, reason)
    end
  end

  post "/internal/result" do
    job_id = conn.body_params["job_id"]
    started_at = conn.body_params["started_at"]
    worker_name = conn.body_params["worker_name"]
    status = conn.body_params["status"]
    result = conn.body_params["result"]
    error = conn.body_params["error"]

    cond do
      not is_binary(job_id) or job_id == "" ->
        json(conn, 422, %{error: "job_id is required"})

      not is_binary(started_at) or started_at == "" ->
        json(conn, 422, %{error: "started_at is required"})

      status not in ["success", "failed"] ->
        json(conn, 422, %{error: "status must be success or failed"})

      true ->
        case JobStore.get(job_id) do
          {:ok, _job} ->
            case JobStore.complete(job_id, started_at, %{
                   "status" => status,
                   "result" => result,
                   "error" => error,
                   "worker_name" => worker_name,
                   "rate_limit_wait_ms" => conn.body_params["rate_limit_wait_ms"]
                 }) do
              {:ok, _} -> send_resp(conn, 204, "")
              {:error, :stale_attempt} -> json(conn, 409, %{error: "stale job attempt"})
              {:error, :invalid_transition} -> json(conn, 409, %{error: "invalid job state"})
              {:error, reason} -> redis_error(conn, reason)
            end

          {:error, :not_found} ->
            json(conn, 404, %{error: "job not found"})

          {:error, reason} ->
            redis_error(conn, reason)
        end
    end
  end

  match _ do
    json(conn, 404, %{error: "not found"})
  end

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(body))
  end

  defp redis_error(conn, reason) do
    json(conn, 500, %{error: "redis error", detail: inspect(reason)})
  end

  defp valid_job_type?(job_type) do
    is_binary(job_type) and String.match?(job_type, ~r/^[a-z0-9_]+$/)
  end
end
