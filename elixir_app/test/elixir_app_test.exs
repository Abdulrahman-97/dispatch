defmodule ElixirAppTest do
  use ExUnit.Case

  test "basic truth" do
    assert true
  end

  test "job status includes worker attribution when available" do
    status =
      Dispatch.Coordinator.JobStore.format_status("job-1", %{
        "status" => "success",
        "result" => "{}",
        "error" => "",
        "worker_name" => "findash-stocks-worker-1",
        "resources" => ~s({"api_slots":1,"memory_slots":1}),
        "worker_resources" => ~s({"api_slots":50,"memory_slots":8}),
        "rate_limit_key" => "fmp_api",
        "rate_limit_cost" => "1",
        "rate_limits" => ~s({"fmp_api":1}),
        "rate_limit_wait_ms" => "1000"
      })

    assert status.worker_name == "findash-stocks-worker-1"
    assert status.resources == %{"api_slots" => 1, "memory_slots" => 1}
    assert status.worker_resources == %{"api_slots" => 50, "memory_slots" => 8}
    assert status.rate_limit_key == "fmp_api"
    assert status.rate_limit_cost == 1
    assert status.rate_limits == %{"fmp_api" => 1}
    assert status.rate_limit_wait_ms == 1000
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

  test "job resources default to one default slot" do
    assert Dispatch.Resources.requirements_from_params(%{}) == {:ok, %{"default_slots" => 1}}
  end

  test "job resources parse generic resource requirements" do
    assert Dispatch.Resources.requirements_from_params(%{
             "resources" => %{"api_slots" => 1, "memory_slots" => "2"}
           }) == {:ok, %{"api_slots" => 1, "memory_slots" => 2}}
  end

  test "worker resources default to WORKER_CONCURRENCY when not configured" do
    previous = System.get_env("DISPATCH_WORKER_RESOURCES_JSON")

    try do
      System.delete_env("DISPATCH_WORKER_RESOURCES_JSON")

      assert Dispatch.Resources.worker_capacity_from_env(3) == {:ok, %{"default_slots" => 3}}
    after
      restore_env("DISPATCH_WORKER_RESOURCES_JSON", previous)
    end
  end

  test "resource fit requires all keys and enough available capacity" do
    requirements = %{"api_slots" => 1, "memory_slots" => 2}

    assert Dispatch.Resources.fits?(requirements, %{"api_slots" => 1, "memory_slots" => 2})
    refute Dispatch.Resources.fits?(requirements, %{"api_slots" => 1, "memory_slots" => 1})
    assert Dispatch.Resources.missing_keys(requirements, %{"api_slots" => 1}) == ["memory_slots"]
  end

  test "available resources may include exhausted zero-capacity keys" do
    assert Dispatch.Resources.normalize_available_resource_map(%{
             "api_slots" => 0,
             "memory_slots" => 2
           }) == {:ok, %{"api_slots" => 0, "memory_slots" => 2}}
  end

  test "rate_limits object parses generic shared quotas" do
    assert {:ok, specs} =
             Dispatch.RateLimit.specs_from_params(%{
               "rate_limits" => %{"provider_api" => 2, "other_api" => "1"}
             })

    assert Enum.sort_by(specs, & &1.key) == [
             %{key: "other_api", cost: 1},
             %{key: "provider_api", cost: 2}
           ]
  end

  test "job without rate_limit_key skips rate limit acquire" do
    post_json = fn _path, _payload -> flunk("rate limiter should not call coordinator") end

    assert Dispatch.Worker.RateLimiter.acquire(
             %{"job_id" => "job-1", "params" => %{}},
             worker_name: "worker-1",
             post_json: post_json
           ) == {:ok, 0}
  end

  test "job with available rate limit tokens is allowed" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    try do
      assert {:ok, %{allowed: true}} =
               Dispatch.Coordinator.RateLimiter.acquire("fmp_api", 1,
                 limits: rate_limits(limit: 2),
                 now_seconds: 1_700_000_000,
                 command: fixed_window_command(agent)
               )
    after
      Agent.stop(agent)
    end
  end

  test "malformed rate limit config fails clearly" do
    assert Dispatch.Coordinator.RateLimiter.parse_limits("not-json") ==
             {:error, "must be valid JSON"}

    assert Dispatch.Coordinator.RateLimiter.parse_limits(~s({"fmp_api":{"window_seconds":60}})) ==
             {:error, "fmp_api: limit is required"}
  end

  test "exhausted rate limit tokens are rejected without consuming more tokens" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    try do
      opts = [
        limits: rate_limits(limit: 1),
        now_seconds: 1_700_000_000,
        command: fixed_window_command(agent)
      ]

      assert {:ok, %{allowed: true}} =
               Dispatch.Coordinator.RateLimiter.acquire("fmp_api", 1, opts)

      assert {:ok, %{allowed: false}} =
               Dispatch.Coordinator.RateLimiter.acquire("fmp_api", 1, opts)

      assert Agent.get(agent, &Map.values/1) == [1]
    after
      Agent.stop(agent)
    end
  end

  test "workers share the same Redis rate limit window" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    try do
      opts = [
        limits: rate_limits(limit: 1),
        now_seconds: 1_700_000_000,
        command: fixed_window_command(agent)
      ]

      assert {:ok, %{allowed: true}} =
               Dispatch.Coordinator.RateLimiter.acquire("fmp_api", 1, opts)

      assert {:ok, %{allowed: false}} =
               Dispatch.Coordinator.RateLimiter.acquire("fmp_api", 1, opts)
    after
      Agent.stop(agent)
    end
  end

  test "worker waits and retries when rate limit is exhausted" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    parent = self()

    post_json = fn "/internal/rate_limit/acquire", _payload ->
      Agent.get_and_update(attempts, fn
        0 -> {{:ok, 429, ~s({"allowed":false,"retry_interval_ms":5})}, 1}
        value -> {{:ok, 200, ~s({"allowed":true})}, value + 1}
      end)
    end

    sleep = fn ms -> send(parent, {:slept, ms}) end

    try do
      assert Dispatch.Worker.RateLimiter.acquire(
               %{
                 "job_id" => "job-1",
                 "params" => %{"rate_limit_key" => "fmp_api", "rate_limit_cost" => 1}
               },
               worker_name: "worker-1",
               post_json: post_json,
               sleep: sleep
             ) == {:ok, 5}

      assert_received {:slept, 5}
      assert Agent.get(attempts, & &1) == 2
    after
      Agent.stop(attempts)
    end
  end

  test "invalid rate_limit_cost fails clearly" do
    assert Dispatch.Worker.RateLimiter.acquire(
             %{
               "job_id" => "job-1",
               "params" => %{"rate_limit_key" => "fmp_api", "rate_limit_cost" => 0}
             },
             worker_name: "worker-1",
             post_json: fn _path, _payload -> flunk("invalid cost should fail locally") end
           ) == {:error, "rate_limit_cost must be a positive integer"}
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

  defp rate_limits(opts) do
    %{
      "fmp_api" => %{
        limit: Keyword.fetch!(opts, :limit),
        window_seconds: 60,
        retry_interval_ms: 5
      }
    }
  end

  defp fixed_window_command(agent) do
    fn ["EVAL", _script, "1", redis_key, limit, cost, _ttl] ->
      limit = String.to_integer(limit)
      cost = String.to_integer(cost)

      Agent.get_and_update(agent, fn state ->
        current = Map.get(state, redis_key, 0)

        if current + cost > limit do
          {{:ok, [0, current]}, state}
        else
          next_value = current + cost
          {{:ok, [1, next_value]}, Map.put(state, redis_key, next_value)}
        end
      end)
    end
  end
end
