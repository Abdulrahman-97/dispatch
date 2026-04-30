defmodule Dispatch.Coordinator.JobQueue do
  @moduledoc false

  import Bitwise

  alias Dispatch.Coordinator.JobStore

  @queue_key "jobs:queue"
  @processing_key "jobs:processing"
  @redis_name Dispatch.Redis

  def enqueue(job_type, params) do
    job_id = generate_job_id()
    payload_json = Jason.encode!(%{job_type: job_type, params: params})

    with {:ok, _} <- JobStore.put_new(job_id, payload_json),
         {:ok, _} <- Redix.command(@redis_name, ["RPUSH", @queue_key, job_id]) do
      {:ok, job_id}
    end
  end

  def claim_next do
    case Redix.command(@redis_name, claim_command()) do
      {:ok, nil} ->
        :empty

      {:ok, job_id} ->
        started_at = now_iso8601()

        case JobStore.mark_running(job_id, started_at) do
          {:ok, :running} ->
            claimed_job(job_id, started_at)

          {:error, reason} ->
            _ = remove_from_processing(job_id)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def claim_command do
    ["RPOPLPUSH", @queue_key, @processing_key]
  end

  def processing_job_ids do
    Redix.command(@redis_name, ["LRANGE", @processing_key, "0", "-1"])
  end

  def remove_from_processing(job_id) do
    Redix.command(@redis_name, ["LREM", @processing_key, "1", job_id])
  end

  defp claimed_job(job_id, started_at) do
    with {:ok, payload} <- JobStore.payload(job_id),
         job_type when is_binary(job_type) <- Map.get(payload, "job_type"),
         params when is_map(params) <- Map.get(payload, "params", %{}) do
      {:ok, %{job_id: job_id, job_type: job_type, params: params, started_at: started_at}}
    else
      _ ->
        _ =
          JobStore.complete(job_id, started_at, %{
            "status" => "failed",
            "result" => nil,
            "error" => "invalid job payload"
          })

        {:error, :invalid_job_payload}
    end
  end

  defp generate_job_id do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    version = bor(band(c, 0x0FFF), 0x4000)
    variant = bor(band(d, 0x3FFF), 0x8000)

    :io_lib.format(
      "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
      [a, b, version, variant, e]
    )
    |> IO.iodata_to_binary()
  end

  defp now_iso8601 do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
