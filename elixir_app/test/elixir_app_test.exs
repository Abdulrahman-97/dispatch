defmodule ElixirAppTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  test "basic truth" do
    assert true
  end

  test "job status includes worker attribution when available" do
    status =
      Dispatch.Coordinator.JobStore.format_status("job-1", %{
        "job_type" => "dagster_run",
        "status" => "success",
        "result" => "{}",
        "error" => "",
        "inserted_at" => "2026-05-02T10:00:00Z",
        "started_at" => "2026-05-02T10:00:02Z",
        "heartbeat_at" => "2026-05-02T10:00:06Z",
        "finished_at" => "2026-05-02T10:00:07Z",
        "worker_name" => "findash-stocks-worker-1",
        "worker_instance_id" => "container-abc",
        "worker_version" => "0.1.0",
        "attempt" => "2",
        "recovery_count" => "1",
        "last_worker_name" => "findash-stocks-worker-old",
        "last_worker_instance_id" => "container-old",
        "last_worker_lost_at" => "2026-05-02T09:59:00Z",
        "group_id" => "group-1",
        "dagster_run_id" => "dagster-run-1",
        "dagster_job_name" => "daily_prices",
        "dagster_code_location" => "stocks",
        "command" => ~s(["dagster","api","execute_run"]),
        "image" => "stocks-worker:test",
        "deployment_revision" => "abc123",
        "metadata" => ~s({"dagster_job_name":"daily_prices"}),
        "log_location" => "s3://logs/dagster/storage/dagster-run-1/compute_logs",
        "exit_code" => "0",
        "logs_tail" => "run finished",
        "failure_category" => "",
        "cancel_requested" => "0",
        "cancel_requested_at" => "",
        "cancellation_acknowledged_at" => "",
        "resources" => ~s({"api_slots":1,"memory_slots":1}),
        "worker_resources" => ~s({"api_slots":50,"memory_slots":8}),
        "rate_limit_key" => "fmp_api",
        "rate_limit_cost" => "1",
        "rate_limits" => ~s({"fmp_api":1}),
        "rate_limit_wait_ms" => "1000"
      })

    assert status.worker_name == "findash-stocks-worker-1"
    assert status.worker_instance_id == "container-abc"
    assert status.worker_version == "0.1.0"
    assert status.attempt == 2
    assert status.recovery_count == 1
    assert status.last_worker_name == "findash-stocks-worker-old"
    assert status.last_worker_instance_id == "container-old"
    assert status.last_worker_lost_at == "2026-05-02T09:59:00Z"
    assert status.job_type == "dagster_run"
    assert status.resources == %{"api_slots" => 1, "memory_slots" => 1}
    assert status.worker_resources == %{"api_slots" => 50, "memory_slots" => 8}
    assert status.group_id == "group-1"
    assert status.dagster_run_id == "dagster-run-1"
    assert status.dagster_job_name == "daily_prices"
    assert status.dagster_code_location == "stocks"
    assert status.command == ["dagster", "api", "execute_run"]
    assert status.image == "stocks-worker:test"
    assert status.deployment_revision == "abc123"
    assert status.metadata == %{"dagster_job_name" => "daily_prices"}
    assert status.log_location == "s3://logs/dagster/storage/dagster-run-1/compute_logs"
    assert status.exit_code == 0
    assert status.logs_tail == "run finished"
    refute status.cancel_requested
    assert status.rate_limit_key == "fmp_api"
    assert status.rate_limit_cost == 1
    assert status.rate_limits == %{"fmp_api" => 1}
    assert status.rate_limit_wait_ms == 1000
    assert status.heartbeat_at == "2026-05-02T10:00:06Z"
    assert status.queue_wait_ms == 2_000
    assert status.worker_duration_ms == 5_000
    assert status.result_size_bytes == 2
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

  test "dagster_run params validate command, env, image, and metadata" do
    assert {:ok, normalized} =
             Dispatch.Coordinator.DagsterRun.validate_params(%{
               "dagster_run_id" => "run-1",
               "command" => ["dagster", "api", "execute_run", "payload"],
               "env" => %{"DAGSTER_HOME" => "/opt/dagster", "RETRY" => 1, "DEBUG" => false},
               "image" => "stocks-worker:test",
               "metadata" => %{"dagster_job_name" => "daily_prices"},
               "resources" => %{"cpu_slots" => 1}
             })

    assert normalized["dagster_run_id"] == "run-1"
    assert normalized["command"] == ["dagster", "api", "execute_run", "payload"]

    assert normalized["env"] == %{
             "DAGSTER_HOME" => "/opt/dagster",
             "RETRY" => "1",
             "DEBUG" => "false"
           }

    assert normalized["image"] == "stocks-worker:test"
    assert normalized["metadata"] == %{"dagster_job_name" => "daily_prices"}
    assert normalized["resources"] == %{"cpu_slots" => 1}
  end

  test "dagster_run params reject invalid command" do
    assert Dispatch.Coordinator.DagsterRun.validate_params(%{
             "dagster_run_id" => "run-1",
             "command" => []
           }) == {:error, "command must be a non-empty array of strings"}
  end

  test "idempotency returns existing job id for duplicate dagster run submissions" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    try do
      command = idempotency_command(agent)

      assert Dispatch.Coordinator.Idempotency.reserve("dagster_run", "run-1", "job-1",
               command: command
             ) == {:ok, :reserved}

      assert Dispatch.Coordinator.Idempotency.reserve("dagster_run", "run-1", "job-2",
               command: command
             ) == {:ok, {:existing, "job-1"}}
    after
      Agent.stop(agent)
    end
  end

  test "idempotency lookup finds an existing job by Dagster run id" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    try do
      command = idempotency_command(agent)

      assert Dispatch.Coordinator.Idempotency.reserve("dagster_run", "run-1", "job-1",
               command: command
             ) == {:ok, :reserved}

      assert Dispatch.Coordinator.Idempotency.lookup("dagster_run", "run-1", command: command) ==
               {:ok, "job-1"}

      assert Dispatch.Coordinator.Idempotency.lookup("dagster_run", "missing", command: command) ==
               {:error, :not_found}
    after
      Agent.stop(agent)
    end
  end

  test "dagster_run command success captures exit code and logs tail" do
    result =
      Dispatch.Worker.Executor.run(%{
        "job_type" => "dagster_run",
        "params" => %{
          "dagster_run_id" => "run-1",
          "command" => [python_executable(), "-c", ~s/print("dagster-ok", end="")/],
          "env" => %{}
        }
      })

    assert result["status"] == "success"
    assert result["exit_code"] == 0
    assert result["logs_tail"] == "dagster-ok"
    assert result["failure_category"] == nil
  end

  test "running dagster_run cancellation terminates command and returns canceled status" do
    result =
      Dispatch.Worker.Executor.run(
        %{
          "job_type" => "dagster_run",
          "params" => %{
            "dagster_run_id" => "run-1",
            "command" => [python_executable(), "-c", "import time; time.sleep(5)"],
            "env" => %{}
          }
        },
        cancel_check: fn -> true end,
        cancel_check_interval_ms: 1,
        cancel_timeout_ms: 100
      )

    assert result["status"] == "canceled"
    assert result["error"] == "dagster_run canceled"
    assert result["failure_category"] == "canceled"
  end

  test "dagster_run execution timeout terminates command and returns failed status" do
    result =
      Dispatch.Worker.Executor.run(
        %{
          "job_type" => "dagster_run",
          "params" => %{
            "dagster_run_id" => "run-1",
            "command" => [python_executable(), "-c", "import time; time.sleep(5)"],
            "env" => %{},
            "execution_timeout_seconds" => 1
          }
        },
        cancel_check_interval_ms: 10,
        cancel_timeout_ms: 100
      )

    assert result["status"] == "failed"
    assert result["error"] == "dagster_run exceeded execution timeout"
    assert result["failure_category"] == "timeout"
  end

  test "dagster_run force kills a process that ignores graceful termination" do
    command =
      if match?({:win32, _name}, :os.type()) do
        "import time; time.sleep(5)"
      else
        "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(5)"
      end

    started_at = System.monotonic_time(:millisecond)

    result =
      Dispatch.Worker.Executor.run(
        %{
          "job_type" => "dagster_run",
          "params" => %{
            "dagster_run_id" => "run-force-kill",
            "command" => [python_executable(), "-c", command],
            "env" => %{}
          }
        },
        cancel_check: fn -> true end,
        cancel_check_interval_ms: 25,
        cancel_timeout_ms: 100
      )

    duration_ms = System.monotonic_time(:millisecond) - started_at
    assert result["status"] == "canceled"
    assert result["failure_category"] == "canceled"
    assert duration_ms < 2_000
  end

  test "dagster_run cancellation kills descendant processes before reporting canceled" do
    marker_path =
      Path.join(
        System.tmp_dir!(),
        "dispatch-cancel-marker-#{System.unique_integer([:positive])}"
      )

    child =
      "import os,time; time.sleep(1); " <>
        "open(os.environ['DISPATCH_TEST_MARKER'], 'w').write('still-running')"

    parent =
      if match?({:win32, _name}, :os.type()) do
        "import subprocess,sys,time; " <>
          "subprocess.Popen([sys.executable, '-c', #{inspect(child)}]); time.sleep(5)"
      else
        "import signal,subprocess,sys,time; " <>
          "signal.signal(signal.SIGTERM, signal.SIG_IGN); " <>
          "subprocess.Popen([sys.executable, '-c', #{inspect(child)}]); time.sleep(5)"
      end

    try do
      result =
        Dispatch.Worker.Executor.run(
          %{
            "job_type" => "dagster_run",
            "params" => %{
              "dagster_run_id" => "run-descendant-cancel",
              "command" => [python_executable(), "-c", parent],
              "env" => %{"DISPATCH_TEST_MARKER" => marker_path}
            }
          },
          cancel_check: fn -> true end,
          cancel_check_interval_ms: 100,
          cancel_timeout_ms: 100,
          force_kill_timeout_ms: 500
        )

      assert result["status"] == "canceled"
      assert result["failure_category"] == "canceled"
      Process.sleep(1_250)
      refute File.exists?(marker_path)
    after
      File.rm(marker_path)
    end
  end

  test "dagster_run never reports canceled before process exit is confirmed" do
    result =
      Dispatch.Worker.Executor.run(
        %{
          "job_type" => "dagster_run",
          "params" => %{
            "dagster_run_id" => "run-unconfirmed-cancel",
            "command" => [python_executable(), "-c", "import time; time.sleep(0.5)"],
            "env" => %{}
          }
        },
        cancel_check: fn -> true end,
        cancel_check_interval_ms: 10,
        cancel_timeout_ms: 10,
        force_kill_timeout_ms: 25,
        terminate_process: fn _pid, _mode -> :ok end
      )

    assert result["status"] == "failed"
    assert result["failure_category"] == "cancellation_termination_unconfirmed"
    assert result["error"] =~ "could not confirm process exit"
    Process.sleep(550)
  end

  test "dagster_run cancellation is not starved by continuous output" do
    started_at = System.monotonic_time(:millisecond)

    result =
      Dispatch.Worker.Executor.run(
        %{
          "job_type" => "dagster_run",
          "params" => %{
            "dagster_run_id" => "run-continuous-output",
            "command" => [
              python_executable(),
              "-c",
              "import sys,time\nwhile True:\n print('x' * 4096, flush=True)\n time.sleep(0.001)"
            ],
            "env" => %{}
          }
        },
        cancel_check: fn -> true end,
        cancel_check_interval_ms: 25,
        cancel_timeout_ms: 100
      )

    duration_ms = System.monotonic_time(:millisecond) - started_at
    assert result["status"] == "canceled"
    assert result["failure_category"] == "canceled"
    assert duration_ms < 2_000
  end

  test "dagster_run logs tail redacts secrets and remains bounded" do
    secret = "super-secret-value"
    noisy = String.duplicate("x", 20_000)

    result =
      Dispatch.Worker.Executor.run(%{
        "job_type" => "dagster_run",
        "params" => %{
          "dagster_run_id" => "run-1",
          "command" => [
            python_executable(),
            "-c",
            ~s/print("#{noisy} API_KEY=#{secret}", end="")/
          ],
          "env" => %{}
        }
      })

    refute result["logs_tail"] =~ secret
    assert result["logs_tail"] =~ "API_KEY=[REDACTED]"
    assert String.length(result["logs_tail"]) <= 16_400
  end

  test "structured observability logs redact sensitive values and bound error text" do
    log =
      capture_log(fn ->
        Dispatch.Observability.event("probe", %{
          dagster_run_id: "run-1",
          error: "token=abc123 " <> String.duplicate("x", 5_000),
          password: "do-not-log"
        })
      end)

    refute log =~ "abc123"
    refute log =~ "do-not-log"
    assert log =~ "token=[REDACTED]"
    assert log =~ ~s("password":"[REDACTED]")
    assert String.length(log) < 5_000
  end

  test "worker in draining mode does not poll for new jobs" do
    refute Dispatch.Worker.Scheduler.should_poll?(%{
             draining: true,
             available: %{"default_slots" => 1}
           })

    assert Dispatch.Worker.Scheduler.should_poll?(%{
             draining: false,
             available: %{"default_slots" => 1}
           })

    refute Dispatch.Worker.Scheduler.should_poll?(%{
             draining: false,
             available: %{"default_slots" => 0}
           })
  end

  test "job group summary includes aggregate status, workers, metrics, and failures" do
    jobs = [
      %{
        job_id: "job-1",
        status: "success",
        worker_name: "worker-a",
        resources: %{"api_slots" => 1},
        rate_limits: %{"provider_api" => 100},
        rate_limit_wait_ms: 0,
        queued_reason: nil,
        queue_wait_ms: 1_000,
        worker_duration_ms: 10_000,
        result_size_bytes: 100,
        error: nil
      },
      %{
        job_id: "job-2",
        status: "failed",
        worker_name: "worker-b",
        resources: %{"api_slots" => 1},
        rate_limits: %{"provider_api" => 100},
        rate_limit_wait_ms: 2_000,
        queued_reason: nil,
        queue_wait_ms: 2_000,
        worker_duration_ms: 20_000,
        result_size_bytes: nil,
        error: "boom"
      },
      %{
        job_id: "job-3",
        status: "queued",
        worker_name: nil,
        resources: %{"api_slots" => 1},
        rate_limits: %{"provider_api" => 100},
        rate_limit_wait_ms: 1_000,
        queued_reason: "group_concurrency_limit:group-1",
        queue_wait_ms: nil,
        worker_duration_ms: nil,
        result_size_bytes: nil,
        error: nil
      }
    ]

    summary =
      Dispatch.Coordinator.JobGroup.summarize(
        "group-1",
        %{"group_key" => "employee-count", "group_concurrency" => "2", "total_jobs" => "3"},
        jobs
      )

    assert summary.group_id == "group-1"
    assert summary.group_key == "employee-count"
    assert summary.group_concurrency == 2
    assert summary.status == "failed"
    assert summary.counts["success"] == 1
    assert summary.counts["failed"] == 1
    assert summary.counts["queued"] == 1
    assert summary.worker_split == %{"worker-a" => 1, "worker-b" => 1}
    assert summary.metrics.queue_wait_ms.p50 == 1_000
    assert summary.metrics.worker_duration_ms.p95 == 20_000
    assert summary.metrics.rate_limit_wait_ms_total == 3_000
    assert summary.failures == [%{job_id: "job-2", error: "boom", worker_name: "worker-b"}]
    assert Enum.map(summary.jobs, & &1.job_id) == ["job-1", "job-2", "job-3"]
    refute Map.has_key?(hd(summary.jobs), :result)
  end

  test "job group summary reports success only when all child jobs succeeded" do
    summary =
      Dispatch.Coordinator.JobGroup.summarize(
        "group-1",
        %{"group_key" => "", "group_concurrency" => "", "total_jobs" => "2"},
        [
          %{job_id: "job-1", status: "success"},
          %{job_id: "job-2", status: "success"}
        ]
      )

    assert summary.status == "success"
    assert summary.group_key == nil
    assert summary.group_concurrency == nil
    assert summary.counts["success"] == 2
  end

  test "coordinator recovery threshold is configurable" do
    started_at = DateTime.utc_now() |> DateTime.add(-240, :second) |> DateTime.to_iso8601()
    now = DateTime.utc_now()

    assert Dispatch.Coordinator.Recovery.older_than_threshold?(started_at, now, 180)
    refute Dispatch.Coordinator.Recovery.older_than_threshold?(started_at, now, 1_800)
  end

  test "coordinator recovery uses the latest attempt heartbeat" do
    fields = %{
      "started_at" => "2026-05-02T10:00:00Z",
      "heartbeat_at" => "2026-05-02T10:04:59Z"
    }

    assert Dispatch.Coordinator.JobStore.processing_heartbeat_at(fields) ==
             "2026-05-02T10:04:59Z"

    assert Dispatch.Coordinator.JobStore.processing_heartbeat_at(%{
             "started_at" => "2026-05-02T10:00:00Z"
           }) == "2026-05-02T10:00:00Z"
  end

  test "lost Dagster runs require reconciliation instead of automatic requeue" do
    assert Dispatch.Coordinator.Recovery.recovery_action(%{
             "job_type" => "dagster_run"
           }) == :mark_worker_lost

    assert Dispatch.Coordinator.Recovery.recovery_action(%{
             "job_type" => "python_callable"
           }) == :requeue
  end

  test "worker loss before process start uses the claim heartbeat and requires reconciliation" do
    fields = %{"started_at" => "2026-05-02T10:00:00Z", "job_type" => "dagster_run"}
    now = ~U[2026-05-02 10:31:00Z]

    heartbeat_at = Dispatch.Coordinator.JobStore.processing_heartbeat_at(fields)

    assert heartbeat_at == "2026-05-02T10:00:00Z"
    assert Dispatch.Coordinator.Recovery.older_than_threshold?(heartbeat_at, now, 1_800)
    assert Dispatch.Coordinator.Recovery.recovery_action(fields) == :mark_worker_lost
  end

  test "worker loss during execution uses the latest stale heartbeat and requires reconciliation" do
    fields = %{
      "started_at" => "2026-05-02T10:00:00Z",
      "heartbeat_at" => "2026-05-02T10:10:00Z",
      "job_type" => "dagster_run"
    }

    now = ~U[2026-05-02 10:41:00Z]
    heartbeat_at = Dispatch.Coordinator.JobStore.processing_heartbeat_at(fields)

    assert Dispatch.Coordinator.Recovery.older_than_threshold?(heartbeat_at, now, 1_800)
    assert Dispatch.Coordinator.Recovery.recovery_action(fields) == :mark_worker_lost
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

  defp idempotency_command(agent) do
    fn
      ["SET", key, value, "NX"] ->
        Agent.get_and_update(agent, fn state ->
          if Map.has_key?(state, key) do
            {{:ok, nil}, state}
          else
            {{:ok, "OK"}, Map.put(state, key, value)}
          end
        end)

      ["GET", key] ->
        {:ok, Agent.get(agent, &Map.get(&1, key))}

      ["EVAL", _script, "1", key, value] ->
        Agent.get_and_update(agent, fn state ->
          if Map.get(state, key) == value do
            {{:ok, 1}, Map.delete(state, key)}
          else
            {{:ok, 0}, state}
          end
        end)
    end
  end

  defp python_executable do
    System.find_executable("python") || System.find_executable("python3") || "python"
  end
end
