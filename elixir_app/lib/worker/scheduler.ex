defmodule Dispatch.Worker.Scheduler do
  @moduledoc false

  use GenServer

  require Logger

  alias Dispatch.Resources
  alias Dispatch.Observability
  alias Dispatch.Worker.Executor

  @http_options [timeout: 5_000]
  @request_options [body_format: :binary]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def should_poll?(%{draining: true}), do: false
  def should_poll?(%{available: available}), do: any_available?(available)

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    :os.set_signal(:sigterm, :handle)

    capacity = worker_capacity!()

    state = %{
      capacity: capacity,
      available: capacity,
      poll_interval_ms: poll_interval_ms(),
      coordinator_url: coordinator_url(),
      worker_name: worker_name(),
      worker_version: worker_version(),
      worker_instance_id: worker_instance_id(),
      running: %{},
      draining: false
    }

    Observability.event("worker_started", %{
      worker_name: state.worker_name,
      worker_instance_id: state.worker_instance_id,
      worker_version: state.worker_version,
      resources: capacity
    })

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    if should_poll?(state) do
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
    state = %{state | running: Map.delete(state.running, job["job_id"])}

    payload =
      result
      |> Map.put("job_id", job["job_id"])
      |> Map.put("started_at", job["started_at"])
      |> Map.put("worker_name", state.worker_name)
      |> Map.put("worker_version", state.worker_version)
      |> Map.put("worker_instance_id", state.worker_instance_id)

    Observability.event(
      "process_exited",
      correlation(job, state)
      |> Map.merge(%{
        status: result["status"],
        exit_code: result["exit_code"],
        failure_category: result["failure_category"]
      })
    )

    maybe_log_upload_completed(job, state, result)
    report_result(state, job, payload)

    send(self(), :poll)
    {:noreply, state}
  end

  def handle_info({:signal, :sigterm}, state) do
    Observability.event(
      "worker_shutdown_started",
      %{
        worker_name: state.worker_name,
        worker_instance_id: state.worker_instance_id,
        active_job_ids: Map.keys(state.running),
        reason: "sigterm"
      },
      :warning
    )

    {:noreply, %{state | draining: true}}
  end

  @impl true
  def terminate(reason, state) do
    Enum.each(state.running, fn {_job_id, job} ->
      Observability.event(
        "worker_shutdown_during_execution",
        Map.put(correlation(job, state), :reason, inspect(reason)),
        :warning
      )

      payload = %{
        "job_id" => job["job_id"],
        "started_at" => job["started_at"],
        "worker_name" => state.worker_name,
        "worker_instance_id" => state.worker_instance_id,
        "reason" => "worker interrupted: #{inspect(reason)}"
      }

      _ = post_json(state.coordinator_url, "/internal/interrupted", payload)
    end)

    :ok
  end

  defp poll_once(state) do
    payload = %{
      "worker_name" => state.worker_name,
      "worker_version" => state.worker_version,
      "worker_instance_id" => state.worker_instance_id,
      "draining" => state.draining,
      "resource_capacity" => state.capacity,
      "available_resources" => state.available
    }

    case post_json(state.coordinator_url, "/internal/poll", payload) do
      {:ok, 200, body} ->
        with {:ok, job} <- Jason.decode(body),
             {:ok, requirements} <-
               Resources.requirements_from_params(Map.get(job, "params", %{})),
             true <- Resources.fits?(requirements, state.available) do
          Observability.event("worker_assigned", correlation(job, state))
          parent = self()

          Task.start(fn ->
            Observability.event("process_started", correlation(job, state))
            send(parent, {:job_finished, job, requirements, safe_execute(job, state)})
          end)

          {:job_started,
           %{
             state
             | available: Resources.subtract(state.available, requirements),
               running: Map.put(state.running, job["job_id"], job)
           }}
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

  defp safe_execute(job, state) do
    result =
      try do
        Executor.run(job,
          cancel_check: fn ->
            attempt_should_stop?(state, job)
          end
        )
      rescue
        exception ->
          %{
            "status" => "failed",
            "result" => nil,
            "error" => "executor crashed: #{Exception.message(exception)}",
            "failure_category" => "executor_crash"
          }
      catch
        kind, reason ->
          %{
            "status" => "failed",
            "result" => nil,
            "error" => "executor crashed: #{kind}: #{inspect(reason)}",
            "failure_category" => "executor_crash"
          }
      end

    sanitize_result(result)
  end

  defp attempt_should_stop?(state, job) do
    case post_json(state.coordinator_url, "/internal/heartbeat", %{
           "job_id" => job["job_id"],
           "started_at" => job["started_at"],
           "worker_name" => state.worker_name,
           "worker_instance_id" => state.worker_instance_id
         }) do
      {:ok, 204, _body} ->
        maybe_log_heartbeat(job, state)
        false

      {:ok, 409, body} ->
        Observability.event(
          "cancellation_acknowledged",
          Map.put(correlation(job, state), :coordinator_response, body)
        )

        true

      {:ok, status, body} ->
        Observability.event(
          "heartbeat_failed",
          correlation(job, state)
          |> Map.merge(%{http_status: status, coordinator_response: body}),
          :warning
        )

        false

      {:error, reason} ->
        Observability.event(
          "heartbeat_failed",
          Map.put(correlation(job, state), :reason, inspect(reason)),
          :warning
        )

        false
    end
  end

  defp report_result(state, job, payload, attempt \\ 1) do
    case post_json(state.coordinator_url, "/internal/result", payload) do
      {:ok, 204, _body} ->
        Observability.event(
          "result_reported",
          correlation(job, state)
          |> Map.merge(%{
            status: payload["status"],
            failure_category: payload["failure_category"],
            report_attempt: attempt
          })
        )

        :ok

      {:ok, 409, body} ->
        Observability.event(
          "result_report_rejected",
          correlation(job, state)
          |> Map.merge(%{report_attempt: attempt, coordinator_response: body}),
          :warning
        )

        :stale

      response ->
        if attempt < result_report_max_attempts() do
          Observability.event(
            "result_reporting_retry",
            correlation(job, state)
            |> Map.merge(%{report_attempt: attempt, reason: inspect(response)}),
            :warning
          )

          Process.sleep(result_report_retry_delay_ms(attempt))
          report_result(state, job, payload, attempt + 1)
        else
          Observability.event(
            "result_reporting_failed",
            correlation(job, state)
            |> Map.merge(%{
              report_attempt: attempt,
              reason: inspect(response),
              final_status: payload["status"]
            }),
            :error
          )

          :error
        end
    end
  end

  defp sanitize_result(result) do
    result
    |> Map.update("error", nil, &Observability.sanitize_error/1)
    |> Map.update("logs_tail", nil, &Observability.sanitize_logs_tail/1)
  end

  defp maybe_log_heartbeat(job, state) do
    key = {:dispatch_heartbeat_log, job["job_id"]}
    now_ms = System.monotonic_time(:millisecond)
    last_logged_ms = Process.get(key)

    if is_nil(last_logged_ms) or now_ms - last_logged_ms >= heartbeat_log_interval_ms() do
      Process.put(key, now_ms)
      Observability.event("heartbeat", correlation(job, state))
    end
  end

  defp maybe_log_upload_completed(job, state, %{"status" => "success"}) do
    log_location =
      job
      |> Map.get("params", %{})
      |> Map.get("metadata", %{})
      |> Map.get("log_location")

    if is_binary(log_location) and log_location != "" do
      Observability.event(
        "log_upload_completed",
        correlation(job, state)
        |> Map.merge(%{
          log_location: log_location,
          verification: "dagster_process_completed_without_launcher_error"
        })
      )
    end
  end

  defp maybe_log_upload_completed(_job, _state, _result), do: :ok

  defp correlation(job, state) do
    params = Map.get(job, "params", %{})
    metadata = Map.get(params, "metadata", %{})

    %{
      dispatch_job_id: job["job_id"],
      dagster_run_id: params["dagster_run_id"],
      dagster_job_name: metadata["dagster_job_name"],
      dagster_code_location: metadata["dagster_code_location"],
      deployment_revision: metadata["deployment_revision"],
      attempt: job["attempt"],
      attempt_started_at: job["started_at"],
      worker_name: state.worker_name,
      worker_instance_id: state.worker_instance_id,
      worker_version: state.worker_version
    }
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

  defp heartbeat_log_interval_ms do
    System.get_env("DISPATCH_HEARTBEAT_LOG_INTERVAL_MS", "60000")
    |> String.to_integer()
  end

  defp result_report_max_attempts do
    System.get_env("DISPATCH_RESULT_REPORT_MAX_ATTEMPTS", "5")
    |> String.to_integer()
    |> max(1)
  end

  defp result_report_retry_delay_ms(attempt) do
    base_delay =
      System.get_env("DISPATCH_RESULT_REPORT_RETRY_BASE_MS", "250")
      |> String.to_integer()
      |> max(1)

    min(base_delay * Integer.pow(2, attempt - 1), 5_000)
  end

  defp worker_concurrency do
    System.get_env("WORKER_CONCURRENCY", "5")
    |> String.to_integer()
  end

  defp worker_name do
    System.get_env("WORKER_NAME") || "worker-1"
  end

  defp worker_instance_id do
    System.get_env("DISPATCH_WORKER_INSTANCE_ID") ||
      System.get_env("HOSTNAME") ||
      "worker-instance-#{System.unique_integer([:positive])}"
  end

  defp worker_version do
    System.get_env("DISPATCH_WORKER_VERSION") ||
      to_string(Application.spec(:elixir_app, :vsn))
  end
end
