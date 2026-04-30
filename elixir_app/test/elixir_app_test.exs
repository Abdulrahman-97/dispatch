defmodule ElixirAppTest do
  use ExUnit.Case

  test "basic truth" do
    assert true
  end

  test "coordinator claims jobs with a non-blocking atomic queue move" do
    assert Dispatch.Coordinator.JobQueue.claim_command() == [
             "RPOPLPUSH",
             "jobs:queue",
             "jobs:processing"
           ]
  end

  test "job status includes worker attribution when available" do
    status =
      Dispatch.Coordinator.JobStore.format_status("job-1", %{
        "status" => "success",
        "result" => "{}",
        "error" => "",
        "worker_name" => "findash-stocks-worker-1"
      })

    assert status.worker_name == "findash-stocks-worker-1"
  end

  test "job status keeps worker attribution optional for old jobs" do
    status =
      Dispatch.Coordinator.JobStore.format_status("job-1", %{
        "status" => "queued",
        "result" => "",
        "error" => ""
      })

    assert status.worker_name == nil
  end

  test "coordinator recovery threshold is configurable" do
    started_at = DateTime.utc_now() |> DateTime.add(-240, :second) |> DateTime.to_iso8601()
    now = DateTime.utc_now()

    assert Dispatch.Coordinator.Recovery.older_than_threshold?(started_at, now, 180)
    refute Dispatch.Coordinator.Recovery.older_than_threshold?(started_at, now, 1_800)
  end

  test "coordinator recovery threshold defaults to long-running job safe value" do
    previous_dispatch = System.get_env("DISPATCH_JOB_STUCK_AFTER_SECONDS")
    previous_legacy = System.get_env("JOB_STUCK_AFTER_SECONDS")

    try do
      System.delete_env("DISPATCH_JOB_STUCK_AFTER_SECONDS")
      System.delete_env("JOB_STUCK_AFTER_SECONDS")

      assert Dispatch.Coordinator.Recovery.stuck_after_seconds() == 1_800
    after
      restore_env("DISPATCH_JOB_STUCK_AFTER_SECONDS", previous_dispatch)
      restore_env("JOB_STUCK_AFTER_SECONDS", previous_legacy)
    end
  end

  test "coordinator recovery threshold can be set from environment" do
    previous = System.get_env("DISPATCH_JOB_STUCK_AFTER_SECONDS")

    try do
      System.put_env("DISPATCH_JOB_STUCK_AFTER_SECONDS", "3600")

      assert Dispatch.Coordinator.Recovery.stuck_after_seconds() == 3_600
    after
      restore_env("DISPATCH_JOB_STUCK_AFTER_SECONDS", previous)
    end
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
