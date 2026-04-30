defmodule Dispatch.Worker.Poller do
  @moduledoc false

  use GenServer

  require Logger

  alias Dispatch.Worker.Executor
  alias Dispatch.Worker.RateLimiter

  @http_options [timeout: 5_000]
  @request_options [body_format: :binary]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    state = %{
      slot: Keyword.fetch!(opts, :slot),
      poll_interval_ms: poll_interval_ms(),
      coordinator_url: coordinator_url(),
      worker_name: worker_name(Keyword.fetch!(opts, :slot))
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    next_delay =
      case poll_once(state) do
        :job_processed -> 0
        :no_job -> state.poll_interval_ms
        :error -> state.poll_interval_ms
      end

    Process.send_after(self(), :poll, next_delay)
    {:noreply, state}
  end

  defp poll_once(state) do
    case post_json(state.coordinator_url, "/internal/poll", %{"worker_name" => state.worker_name}) do
      {:ok, 200, body} ->
        case Jason.decode(body) do
          {:ok, job} ->
            Logger.info("worker=#{state.worker_name} job=#{job["job_id"]} picked up")
            result = safe_execute(job, state)

            case post_json(
                   state.coordinator_url,
                   "/internal/result",
                   result
                   |> Map.put("job_id", job["job_id"])
                   |> Map.put("started_at", job["started_at"])
                   |> Map.put("worker_name", state.worker_name)
                 ) do
              {:ok, 204, _body} ->
                Logger.info(
                  "worker=#{state.worker_name} job=#{job["job_id"]} status=#{result["status"]}"
                )

                :job_processed

              {:ok, 409, body} ->
                Logger.warning(
                  "worker=#{state.worker_name} job=#{job["job_id"]} stale_result body=#{inspect(body)}"
                )

                :job_processed

              {:ok, status, body} ->
                Logger.error(
                  "worker=#{state.worker_name} job=#{job["job_id"]} result_submit_failed status=#{status} body=#{inspect(body)}"
                )

                :error

              {:error, reason} ->
                Logger.error(
                  "worker=#{state.worker_name} job=#{job["job_id"]} result_submit_failed reason=#{inspect(reason)}"
                )

                :error
            end

          {:error, reason} ->
            Logger.error("worker=#{state.worker_name} invalid_poll_response=#{inspect(reason)}")
            :error
        end

      {:ok, 204, _body} ->
        :no_job

      {:ok, status, body} ->
        Logger.error(
          "worker=#{state.worker_name} poll_failed status=#{status} body=#{inspect(body)}"
        )

        :error

      {:error, reason} ->
        Logger.error("worker=#{state.worker_name} poll_failed reason=#{inspect(reason)}")
        :error
    end
  end

  defp post_json(base_url, path, payload) do
    url = to_charlist(String.trim_trailing(base_url, "/") <> path)
    body = Jason.encode!(payload)
    headers = [{~c"content-type", ~c"application/json"}]

    case :httpc.request(
           :post,
           {url, headers, ~c"application/json", body},
           @http_options,
           @request_options
         ) do
      {:ok, {{_version, status, _reason_phrase}, _headers, response_body}} ->
        {:ok, status, response_body}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp safe_execute(job, state) do
    try do
      case RateLimiter.acquire(job,
             worker_name: state.worker_name,
             post_json: fn path, payload -> post_json(state.coordinator_url, path, payload) end
           ) do
        {:ok, rate_limit_wait_ms} ->
          job
          |> Executor.run()
          |> Map.put("rate_limit_wait_ms", rate_limit_wait_ms)

        {:error, reason} ->
          %{
            "status" => "failed",
            "result" => nil,
            "error" => reason,
            "rate_limit_wait_ms" => 0
          }
      end
    rescue
      exception ->
        %{
          "status" => "failed",
          "result" => nil,
          "error" => "executor crashed: #{Exception.message(exception)}",
          "rate_limit_wait_ms" => 0
        }
    catch
      kind, reason ->
        %{
          "status" => "failed",
          "result" => nil,
          "error" => "executor crashed: #{kind}: #{inspect(reason)}",
          "rate_limit_wait_ms" => 0
        }
    end
  end

  defp coordinator_url do
    System.get_env("COORDINATOR_URL", "http://localhost:4000")
  end

  defp poll_interval_ms do
    System.get_env("WORKER_POLL_INTERVAL_MS", "1000")
    |> String.to_integer()
  end

  defp worker_name(slot) do
    System.get_env("WORKER_NAME") || "worker-#{slot}"
  end
end
