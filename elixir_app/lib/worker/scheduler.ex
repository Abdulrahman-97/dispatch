defmodule Dispatch.Worker.Scheduler do
  @moduledoc false

  use GenServer

  require Logger

  alias Dispatch.Resources
  alias Dispatch.Worker.Executor

  @http_options [timeout: 5_000]
  @request_options [body_format: :binary]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    capacity = worker_capacity!()

    state = %{
      capacity: capacity,
      available: capacity,
      poll_interval_ms: poll_interval_ms(),
      coordinator_url: coordinator_url(),
      worker_name: worker_name()
    }

    Logger.info("worker=#{state.worker_name} resources=#{inspect(capacity)}")
    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    if any_available?(state.available) do
      case poll_once(state) do
        {:job_started, new_state} ->
          Process.send_after(self(), :poll, 0)
          {:noreply, new_state}

        :no_job ->
          Process.send_after(self(), :poll, state.poll_interval_ms)
          {:noreply, state}

        :error ->
          Process.send_after(self(), :poll, state.poll_interval_ms)
          {:noreply, state}
      end
    else
      Process.send_after(self(), :poll, state.poll_interval_ms)
      {:noreply, state}
    end
  end

  def handle_info({:job_finished, job, requirements, result}, state) do
    state = %{state | available: Resources.add(state.available, requirements, state.capacity)}

    payload =
      result
      |> Map.put("job_id", job["job_id"])
      |> Map.put("started_at", job["started_at"])
      |> Map.put("worker_name", state.worker_name)

    case post_json(state.coordinator_url, "/internal/result", payload) do
      {:ok, 204, _body} ->
        Logger.info("worker=#{state.worker_name} job=#{job["job_id"]} status=#{result["status"]}")

      {:ok, 409, body} ->
        Logger.warning(
          "worker=#{state.worker_name} job=#{job["job_id"]} stale_result body=#{inspect(body)}"
        )

      {:ok, status, body} ->
        Logger.error(
          "worker=#{state.worker_name} job=#{job["job_id"]} result_submit_failed status=#{status} body=#{inspect(body)}"
        )

      {:error, reason} ->
        Logger.error(
          "worker=#{state.worker_name} job=#{job["job_id"]} result_submit_failed reason=#{inspect(reason)}"
        )
    end

    send(self(), :poll)
    {:noreply, state}
  end

  defp poll_once(state) do
    payload = %{
      "worker_name" => state.worker_name,
      "resource_capacity" => state.capacity,
      "available_resources" => state.available
    }

    case post_json(state.coordinator_url, "/internal/poll", payload) do
      {:ok, 200, body} ->
        with {:ok, job} <- Jason.decode(body),
             {:ok, requirements} <-
               Resources.requirements_from_params(Map.get(job, "params", %{})),
             true <- Resources.fits?(requirements, state.available) do
          Logger.info("worker=#{state.worker_name} job=#{job["job_id"]} picked up")
          parent = self()

          Task.start(fn ->
            send(parent, {:job_finished, job, requirements, safe_execute(job)})
          end)

          {:job_started, %{state | available: Resources.subtract(state.available, requirements)}}
        else
          false ->
            Logger.error(
              "worker=#{state.worker_name} invalid_assignment insufficient_local_resources"
            )

            :error

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

  defp safe_execute(job) do
    try do
      Executor.run(job)
    rescue
      exception ->
        %{
          "status" => "failed",
          "result" => nil,
          "error" => "executor crashed: #{Exception.message(exception)}"
        }
    catch
      kind, reason ->
        %{
          "status" => "failed",
          "result" => nil,
          "error" => "executor crashed: #{kind}: #{inspect(reason)}"
        }
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

  defp worker_capacity! do
    case Resources.worker_capacity_from_env(worker_concurrency()) do
      {:ok, resources} -> resources
      {:error, reason} -> raise ArgumentError, "invalid DISPATCH_WORKER_RESOURCES_JSON: #{reason}"
    end
  end

  defp any_available?(available), do: Enum.any?(available, fn {_key, value} -> value > 0 end)

  defp coordinator_url do
    System.get_env("COORDINATOR_URL", "http://localhost:4000")
  end

  defp poll_interval_ms do
    System.get_env("WORKER_POLL_INTERVAL_MS", "1000")
    |> String.to_integer()
  end

  defp worker_concurrency do
    System.get_env("WORKER_CONCURRENCY", "5")
    |> String.to_integer()
  end

  defp worker_name do
    System.get_env("WORKER_NAME") || "worker-1"
  end
end
