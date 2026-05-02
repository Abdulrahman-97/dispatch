defmodule Dispatch.Coordinator.JobGroup do
  @moduledoc false

  alias Dispatch.Coordinator.JobQueue
  alias Dispatch.Coordinator.JobStore
  alias Dispatch.UUID

  @job_type_pattern ~r/^[a-z0-9_]+$/

  def create(attrs) when is_map(attrs) do
    with {:ok, jobs} <- normalize_jobs(Map.get(attrs, "jobs")),
         {:ok, group_key} <- normalize_group_key(Map.get(attrs, "group_key")),
         {:ok, group_concurrency} <-
           normalize_group_concurrency(Map.get(attrs, "group_concurrency")) do
      group_id = UUID.generate()

      with {:ok, _} <-
             JobStore.put_group(group_id, %{
               "group_key" => group_key || "",
               "group_concurrency" => encode_optional_integer(group_concurrency),
               "total_jobs" => Integer.to_string(length(jobs))
             }),
           {:ok, job_ids} <- enqueue_jobs(group_id, jobs) do
        {:ok, %{group_id: group_id, job_ids: job_ids}}
      end
    end
  end

  def create(_attrs), do: {:error, "request body must be a JSON object"}

  def get(group_id) when is_binary(group_id) and group_id != "" do
    with {:ok, group} <- JobStore.get_group(group_id),
         {:ok, job_ids} <- JobStore.group_job_ids(group_id),
         {:ok, jobs} <- fetch_jobs(job_ids) do
      {:ok, summarize(group_id, group, jobs)}
    end
  end

  def get(_group_id), do: {:error, :not_found}

  def summarize(group_id, group, jobs) when is_list(jobs) do
    counts = count_statuses(jobs)
    total_jobs = parse_integer(group["total_jobs"]) || length(jobs)

    %{
      group_id: group_id,
      group_key: normalize_empty(group["group_key"]),
      group_concurrency: parse_integer(group["group_concurrency"]),
      status: group_status(counts, total_jobs),
      counts: counts,
      total_jobs: total_jobs,
      job_ids: Enum.map(jobs, & &1.job_id),
      worker_split: worker_split(jobs),
      metrics: group_metrics(jobs),
      failures: failures(jobs),
      jobs: Enum.map(jobs, &job_summary/1)
    }
  end

  defp normalize_jobs(jobs) when is_list(jobs) and jobs != [] do
    jobs
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {job, index}, {:ok, acc} ->
      case normalize_job(job, index) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized_jobs} -> {:ok, Enum.reverse(normalized_jobs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_jobs(_jobs), do: {:error, "jobs must be a non-empty JSON array"}

  defp normalize_job(job, index) when is_map(job) do
    job_type = Map.get(job, "job_type")
    params = Map.get(job, "params", %{})

    cond do
      not valid_job_type?(job_type) ->
        {:error, "jobs[#{index}].job_type must match ^[a-z0-9_]+$"}

      not is_map(params) ->
        {:error, "jobs[#{index}].params must be a JSON object"}

      true ->
        {:ok, %{job_type: job_type, params: params}}
    end
  end

  defp normalize_job(_job, index), do: {:error, "jobs[#{index}] must be a JSON object"}

  defp normalize_group_key(nil), do: {:ok, nil}

  defp normalize_group_key(group_key) when is_binary(group_key) do
    {:ok, String.trim(group_key)}
  end

  defp normalize_group_key(_group_key), do: {:error, "group_key must be a string"}

  defp normalize_group_concurrency(nil), do: {:ok, nil}
  defp normalize_group_concurrency(""), do: {:ok, nil}

  defp normalize_group_concurrency(value) when is_integer(value) and value > 0 do
    {:ok, value}
  end

  defp normalize_group_concurrency(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _ -> {:error, "group_concurrency must be a positive integer"}
    end
  end

  defp normalize_group_concurrency(_value),
    do: {:error, "group_concurrency must be a positive integer"}

  defp enqueue_jobs(group_id, jobs) do
    jobs
    |> Enum.reduce_while({:ok, []}, fn job, {:ok, acc} ->
      case JobQueue.enqueue(job.job_type, job.params, %{"group_id" => group_id}) do
        {:ok, job_id} -> {:cont, {:ok, [job_id | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, job_ids} -> {:ok, Enum.reverse(job_ids)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_jobs(job_ids) do
    job_ids
    |> Enum.reduce_while({:ok, []}, fn job_id, {:ok, acc} ->
      case JobStore.get(job_id) do
        {:ok, fields} ->
          {:cont, {:ok, [JobStore.format_status(job_id, fields) | acc]}}

        {:error, :not_found} ->
          {:cont, {:ok, [missing_job(job_id) | acc]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp count_statuses(jobs) do
    base = %{"queued" => 0, "running" => 0, "success" => 0, "failed" => 0, "missing" => 0}

    Enum.reduce(jobs, base, fn job, acc ->
      status = Map.get(job, :status) || "missing"
      Map.update(acc, status, 1, &(&1 + 1))
    end)
  end

  defp group_status(counts, total_jobs) do
    cond do
      total_jobs == 0 -> "empty"
      Map.get(counts, "failed", 0) > 0 -> "failed"
      Map.get(counts, "success", 0) == total_jobs -> "success"
      Map.get(counts, "running", 0) > 0 -> "running"
      Map.get(counts, "queued", 0) == total_jobs -> "queued"
      true -> "running"
    end
  end

  defp worker_split(jobs) do
    jobs
    |> Enum.map(&Map.get(&1, :worker_name))
    |> Enum.reject(&is_nil/1)
    |> Enum.frequencies()
  end

  defp group_metrics(jobs) do
    rate_limit_wait_values =
      jobs
      |> Enum.map(&Map.get(&1, :rate_limit_wait_ms))
      |> numeric_values()

    %{
      queue_wait_ms: stats(Enum.map(jobs, &Map.get(&1, :queue_wait_ms))),
      worker_duration_ms: stats(Enum.map(jobs, &Map.get(&1, :worker_duration_ms))),
      rate_limit_wait_ms: stats(rate_limit_wait_values),
      rate_limit_wait_ms_total: Enum.sum(rate_limit_wait_values),
      result_size_bytes: stats(Enum.map(jobs, &Map.get(&1, :result_size_bytes)))
    }
  end

  defp failures(jobs) do
    jobs
    |> Enum.filter(&(Map.get(&1, :status) == "failed"))
    |> Enum.map(fn job ->
      %{
        job_id: job.job_id,
        error: Map.get(job, :error),
        worker_name: Map.get(job, :worker_name)
      }
    end)
  end

  defp job_summary(job) do
    %{
      job_id: job.job_id,
      status: Map.get(job, :status),
      worker_name: Map.get(job, :worker_name),
      resources: Map.get(job, :resources),
      rate_limits: Map.get(job, :rate_limits),
      rate_limit_wait_ms: Map.get(job, :rate_limit_wait_ms),
      queued_reason: Map.get(job, :queued_reason),
      queue_wait_ms: Map.get(job, :queue_wait_ms),
      worker_duration_ms: Map.get(job, :worker_duration_ms),
      result_size_bytes: Map.get(job, :result_size_bytes)
    }
  end

  defp stats(values) do
    values = numeric_values(values)

    if values == [] do
      nil
    else
      sorted = Enum.sort(values)

      %{
        count: length(sorted),
        min: List.first(sorted),
        p50: percentile(sorted, 0.50),
        p95: percentile(sorted, 0.95),
        max: List.last(sorted)
      }
    end
  end

  defp numeric_values(values) do
    Enum.filter(values, &is_number/1)
  end

  defp percentile(sorted_values, percentile) do
    index =
      sorted_values
      |> length()
      |> Kernel.*(percentile)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> Kernel.-(1)

    Enum.at(sorted_values, index)
  end

  defp parse_integer(value) when is_integer(value), do: value

  defp parse_integer(value) when is_binary(value) and value != "" do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> nil
    end
  end

  defp parse_integer(_value), do: nil

  defp encode_optional_integer(nil), do: ""
  defp encode_optional_integer(value), do: Integer.to_string(value)

  defp normalize_empty(""), do: nil
  defp normalize_empty(value), do: value

  defp valid_job_type?(job_type) do
    is_binary(job_type) and String.match?(job_type, @job_type_pattern)
  end

  defp missing_job(job_id) do
    %{
      job_id: job_id,
      status: "missing",
      worker_name: nil,
      resources: nil,
      rate_limits: nil,
      rate_limit_wait_ms: nil,
      queued_reason: "job_missing",
      queue_wait_ms: nil,
      worker_duration_ms: nil,
      result_size_bytes: nil,
      error: "job not found"
    }
  end
end
