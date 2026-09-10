defmodule Dispatch.LifecycleStoreTest do
  use ExUnit.Case, async: false
  alias Dispatch.Coordinator.JobStore
  @moduletag skip: System.get_env("DISPATCH_DISPOSABLE_REDIS_URL") == nil

  setup do
    # Dedicated disposable Redis only. Never use the application's REDIS_URL.
    start_supervised!(
      {Redix, {System.fetch_env!("DISPATCH_DISPOSABLE_REDIS_URL"), [name: Dispatch.Redis]}}
    )

    :ok
  end

  defp queued do
    id = "lifecycle-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             JobStore.put_new(id, "{}", %{
               "job_type" => "dagster_run",
               "dagster_run_id" => id,
               "metadata" => ~s({"parent_run_id":"retained-parent"})
             })

    assert {:ok, _} = Redix.command(Dispatch.Redis, ["RPUSH", "jobs:queue", id])
    id
  end

  defp claim(id),
    do:
      JobStore.claim_queued(
        id,
        "2026-09-10T00:00:00Z",
        "fixture",
        %{"default_slots" => 1},
        [],
        "fixture",
        "instance"
      )

  test "terminal handoff is idempotent, identity-bound, and keeps history" do
    id = queued()
    assert {:ok, "identity_mismatch"} = JobStore.reconcile_terminal(id, "other", "FAILURE")
    assert {:ok, "reconciled"} = JobStore.reconcile_terminal(id, id, "FAILURE")
    assert {:ok, "reconciled"} = JobStore.reconcile_terminal(id, id, "FAILURE")
    assert {:error, :invalid_transition} = claim(id)
    assert {:ok, record} = JobStore.get(id)
    assert record["attempt"] == "0"
    assert record["metadata"] == ~s({"parent_run_id":"retained-parent"})
    assert record["terminal_dagster_status"] == "FAILURE"
  end

  test "HTTP handoff validates the assertion and retains active work" do
    id = queued()

    conn =
      Plug.Test.conn(
        :post,
        "/jobs/#{id}/reconcile-terminal",
        Jason.encode!(%{
          dagster_run_id: id,
          dagster_status: "STARTED"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")

    assert Dispatch.Coordinator.Router.call(conn, []).status == 422
    assert {:ok, :running} = claim(id)

    conn =
      Plug.Test.conn(
        :post,
        "/jobs/#{id}/reconcile-terminal",
        Jason.encode!(%{
          dagster_run_id: id,
          dagster_status: "FAILURE"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")

    response = Dispatch.Coordinator.Router.call(conn, [])
    assert response.status == 200
    assert Jason.decode!(response.resp_body)["reconciliation"] == "retained"
  end

  test "claim and handoff race has exactly one winner" do
    for _ <- 1..30 do
      id = queued()
      claim_task = Task.async(fn -> claim(id) end)
      handoff_task = Task.async(fn -> JobStore.reconcile_terminal(id, id, "FAILURE") end)
      pair = {Task.await(claim_task), Task.await(handoff_task)}

      assert pair in [
               {{:ok, :running}, {:ok, "retained"}},
               {{:error, :invalid_transition}, {:ok, "reconciled"}}
             ]

      {:ok, record} = JobStore.get(id)

      if record["status"] == "running" do
        assert record["attempt"] == "1"
        assert record["cancel_requested"] == "0"
      end
    end
  end

  test "lost receipt ack replays exactly, conflicting and stale callbacks fail" do
    id = queued()
    assert {:ok, :running} = claim(id)
    attrs = %{"status" => "success", "result" => "{}", "exit_code" => 0}
    assert {:ok, :completed} = JobStore.complete(id, "2026-09-10T00:00:00Z", attrs)
    {:ok, first} = JobStore.get(id)
    assert {:ok, :completed} = JobStore.complete(id, "2026-09-10T00:00:00Z", attrs)
    assert {:ok, ^first} = JobStore.get(id)

    assert {:error, :invalid_transition} =
             JobStore.complete(id, "2026-09-10T00:00:00Z", %{attrs | "result" => "conflict"})

    assert {:error, :stale_attempt} = JobStore.complete(id, "other-attempt", attrs)
  end

  test "lost worker is not requeued and late exact result remains recoverable" do
    id = queued()
    assert {:ok, :running} = claim(id)
    assert {:ok, :worker_lost} = JobStore.mark_worker_lost(id, "2026-09-10T00:00:00Z")
    # Coordinator connection/process restart with Redis retained.
    stop_supervised!(Redix)

    start_supervised!(
      {Redix, {System.fetch_env!("DISPATCH_DISPOSABLE_REDIS_URL"), [name: Dispatch.Redis]}}
    )

    assert {:ok, "retained"} = JobStore.reconcile_terminal(id, id, "FAILURE")
    assert {:error, :invalid_transition} = claim(id)

    assert {:ok, :completed} =
             JobStore.complete(id, "2026-09-10T00:00:00Z", %{
               "status" => "success",
               "result" => "{}"
             })
  end

  test "restored queue with prior attempt or processing evidence is retained" do
    for {field, value} <- [{"attempt", "1"}, {"started_at", "old"}, {"worker_instance_id", "old"}] do
      id = queued()
      Redix.command!(Dispatch.Redis, ["HSET", "job:#{id}", field, value])
      assert {:ok, "retained"} = JobStore.reconcile_terminal(id, id, "FAILURE")
    end

    id = queued()
    Redix.command!(Dispatch.Redis, ["LPUSH", "jobs:processing", id])
    assert {:ok, "retained"} = JobStore.reconcile_terminal(id, id, "FAILURE")
  end
end
