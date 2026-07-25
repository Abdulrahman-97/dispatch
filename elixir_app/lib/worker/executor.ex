defmodule Dispatch.Worker.Executor do
  @moduledoc false

  alias Dispatch.Observability

  @dagster_cancel_check_interval_ms 500
  @dagster_cancel_timeout_ms 5_000
  @dagster_force_kill_verify_timeout_ms 5_000
  @default_dagster_run_timeout_seconds 21_600
  @logs_tail_bytes 16_384
  @runner """
  import json
  import subprocess
  import sys

  completed = subprocess.run(
      [sys.executable, sys.argv[1], sys.argv[2]],
      capture_output=True,
      text=True,
  )

  print(
      json.dumps(
          {
              "returncode": completed.returncode,
              "stdout": completed.stdout,
              "stderr": completed.stderr,
          }
      )
  )
  """

  def run(job, opts \\ [])

  def run(%{"job_type" => "dagster_run", "params" => params}, opts) do
    run_dagster(params, opts)
  end

  def run(%{"job_type" => job_type, "params" => params}, _opts) do
    try do
      script_path = Path.join("scripts", "#{job_type}.py")
      python_root = python_root()
      full_script_path = Path.join(python_root, script_path)

      if File.exists?(full_script_path) do
        execute_script(python_root, script_path, params)
      else
        failed_result("script not found: #{script_path}")
      end
    rescue
      exception ->
        failed_result(Exception.message(exception))
    catch
      kind, reason ->
        failed_result("executor #{kind}: #{inspect(reason)}")
    end
  end

  defp run_dagster(params, opts) do
    with [executable | args] <- Map.get(params, "command"),
         true <- is_binary(executable),
         true <- Enum.all?(args, &is_binary/1),
         env when is_map(env) <- Map.get(params, "env", %{}),
         {:ok, timeout_ms} <- dagster_run_timeout_ms(params) do
      execute_dagster_command(
        executable,
        args,
        env,
        Keyword.put_new(opts, :execution_timeout_ms, timeout_ms)
      )
    else
      _ -> failed_result("invalid dagster_run payload")
    end
  rescue
    exception -> failed_result(Exception.message(exception))
  catch
    kind, reason -> failed_result("dagster_run executor #{kind}: #{inspect(reason)}")
  end

  defp execute_dagster_command(executable, args, env, opts) do
    command = [executable | args]
    resolved_executable = System.find_executable(executable) || executable

    case process_group_command(resolved_executable, args) do
      {:ok, {port_executable, port_args}} ->
        env =
          Enum.map(env, fn {key, value} ->
            {to_charlist(key), to_charlist(to_string(value))}
          end)

        port =
          Port.open({:spawn_executable, port_executable}, [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: port_args,
            env: env
          ])

        schedule_attempt_check(port, opts)
        wait_for_port(port, command, opts, "", System.monotonic_time(:millisecond))

      {:error, reason} ->
        failed_result(reason, "process_group_unavailable")
    end
  end

  defp wait_for_port(port, command, opts, logs_tail, started_at_ms) do
    cancel_check = Keyword.get(opts, :cancel_check, fn -> false end)

    receive do
      {^port, {:data, data}} ->
        wait_for_port(port, command, opts, append_tail(logs_tail, data), started_at_ms)

      {^port, {:exit_status, exit_code}} ->
        dagster_result(exit_code, logs_tail)

      {:dispatch_attempt_check, ^port} ->
        cond do
          execution_timed_out?(opts, started_at_ms) ->
            timeout_port(port, command, opts, logs_tail)

          cancel_check.() ->
            cancel_port(port, command, opts, logs_tail)

          true ->
            schedule_attempt_check(port, opts)
            wait_for_port(port, command, opts, logs_tail, started_at_ms)
        end
    end
  end

  defp execution_timed_out?(opts, started_at_ms) do
    timeout_ms = Keyword.fetch!(opts, :execution_timeout_ms)
    System.monotonic_time(:millisecond) - started_at_ms >= timeout_ms
  end

  defp timeout_port(port, command, opts, logs_tail) do
    timeout_ms = Keyword.get(opts, :cancel_timeout_ms, @dagster_cancel_timeout_ms)
    os_pid = port |> port_os_pid() |> signal_target_pid()

    _graceful_signal = signal_process(os_pid, :graceful, opts)
    Process.send_after(self(), {:dispatch_timeout_force_kill, port}, timeout_ms)
    wait_for_timed_out_port(port, command, os_pid, logs_tail, opts)
  end

  defp wait_for_timed_out_port(port, command, os_pid, logs_tail, opts) do
    receive do
      {^port, {:data, data}} ->
        wait_for_timed_out_port(port, command, os_pid, append_tail(logs_tail, data), opts)

      {^port, {:exit_status, exit_code}} ->
        timed_out_result(exit_code, logs_tail)

      {:dispatch_timeout_force_kill, ^port} ->
        force_kill_and_verify(port, command, os_pid, logs_tail, opts, :timeout)
    end
  end

  defp cancel_port(port, command, opts, logs_tail) do
    timeout_ms = Keyword.get(opts, :cancel_timeout_ms, @dagster_cancel_timeout_ms)
    os_pid = port |> port_os_pid() |> signal_target_pid()

    _graceful_signal = signal_process(os_pid, :graceful, opts)
    Process.send_after(self(), {:dispatch_cancel_force_kill, port}, timeout_ms)
    wait_for_canceled_port(port, command, os_pid, logs_tail, opts)
  end

  defp wait_for_canceled_port(port, command, os_pid, logs_tail, opts) do
    receive do
      {^port, {:data, data}} ->
        wait_for_canceled_port(port, command, os_pid, append_tail(logs_tail, data), opts)

      {^port, {:exit_status, exit_code}} ->
        canceled_result(exit_code, logs_tail)

      {:dispatch_cancel_force_kill, ^port} ->
        force_kill_and_verify(port, command, os_pid, logs_tail, opts, :cancellation)
    end
  end

  defp force_kill_and_verify(port, command, os_pid, logs_tail, opts, outcome) do
    force_signal = signal_process(os_pid, :force, opts)

    verification_timeout_ms =
      Keyword.get(
        opts,
        :force_kill_timeout_ms,
        @dagster_force_kill_verify_timeout_ms
      )

    Process.send_after(
      self(),
      {:dispatch_force_kill_verification_timeout, port, outcome},
      verification_timeout_ms
    )

    wait_for_force_killed_port(
      port,
      command,
      logs_tail,
      outcome,
      force_signal
    )
  end

  defp wait_for_force_killed_port(port, command, logs_tail, outcome, force_signal) do
    receive do
      {^port, {:data, data}} ->
        wait_for_force_killed_port(
          port,
          command,
          append_tail(logs_tail, data),
          outcome,
          force_signal
        )

      {^port, {:exit_status, exit_code}} ->
        termination_result(outcome, exit_code, logs_tail)

      {:dispatch_force_kill_verification_timeout, ^port, ^outcome} ->
        unconfirmed_termination_result(outcome, force_signal, logs_tail)
    end
  end

  defp schedule_attempt_check(port, opts) do
    Process.send_after(
      self(),
      {:dispatch_attempt_check, port},
      Keyword.get(opts, :cancel_check_interval_ms, @dagster_cancel_check_interval_ms)
    )
  end

  defp port_os_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when is_integer(pid) -> pid
      _ -> nil
    end
  end

  defp signal_target_pid(nil), do: nil

  defp signal_target_pid(port_pid) do
    if match?({:unix, _name}, :os.type()) do
      child_process_id(port_pid)
    else
      port_pid
    end
  end

  defp child_process_id(parent_pid) do
    pgrep = System.find_executable("pgrep") || "pgrep"

    case System.cmd(
           pgrep,
           ["-P", Integer.to_string(parent_pid)],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        output
        |> String.split()
        |> Enum.find_value(fn value ->
          case Integer.parse(value) do
            {pid, ""} when pid > 0 -> pid
            _ -> nil
          end
        end)

      {_output, _exit_code} ->
        nil
    end
  rescue
    _exception -> nil
  end

  defp signal_process(pid, mode, opts) do
    signaler = Keyword.get(opts, :terminate_process, &terminate_os_process/2)
    signaler.(pid, mode)
  end

  defp terminate_os_process(nil, _mode),
    do: {:error, "process-group id unavailable"}

  defp terminate_os_process(pid, mode) do
    {command, args} =
      if match?({:win32, _name}, :os.type()) do
        taskkill_args =
          case mode do
            :force -> ["/PID", Integer.to_string(pid), "/T", "/F"]
            :graceful -> ["/PID", Integer.to_string(pid), "/T"]
          end

        {"taskkill", taskkill_args}
      else
        signal = if mode == :force, do: "-KILL", else: "-TERM"
        target = if mode == :force, do: "-#{pid}", else: Integer.to_string(pid)
        {System.find_executable("kill") || "kill", [signal, "--", target]}
      end

    case System.cmd(command, args, stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {output, exit_code} ->
        {:error,
         Observability.sanitize_error(
           "signal command exited with code #{exit_code}: #{String.trim(output)}"
         )}
    end
  rescue
    exception ->
      {:error,
       Observability.sanitize_error("signal command failed: #{Exception.message(exception)}")}
  end

  defp dagster_result(0, logs_tail) do
    %{
      "status" => "success",
      "result" => nil,
      "error" => nil,
      "exit_code" => 0,
      "logs_tail" => sanitized_logs_tail(logs_tail),
      "failure_category" => nil
    }
  end

  defp dagster_result(exit_code, logs_tail) do
    %{
      "status" => "failed",
      "result" => nil,
      "error" => "dagster_run command exited with code #{exit_code}",
      "exit_code" => exit_code,
      "logs_tail" => sanitized_logs_tail(logs_tail),
      "failure_category" => "process_exit_nonzero"
    }
  end

  defp canceled_result(exit_code, logs_tail) do
    %{
      "status" => "canceled",
      "result" => nil,
      "error" => "dagster_run canceled",
      "exit_code" => exit_code,
      "logs_tail" => sanitized_logs_tail(logs_tail),
      "failure_category" => "canceled"
    }
  end

  defp timed_out_result(exit_code, logs_tail) do
    %{
      "status" => "failed",
      "result" => nil,
      "error" => "dagster_run exceeded execution timeout",
      "exit_code" => exit_code,
      "logs_tail" => sanitized_logs_tail(logs_tail),
      "failure_category" => "timeout"
    }
  end

  defp termination_result(:cancellation, exit_code, logs_tail),
    do: canceled_result(exit_code, logs_tail)

  defp termination_result(:timeout, exit_code, logs_tail),
    do: timed_out_result(exit_code, logs_tail)

  defp unconfirmed_termination_result(outcome, force_signal, logs_tail) do
    signal_detail =
      case force_signal do
        :ok -> "force signal was accepted"
        {:error, reason} -> "force signal failed: #{reason}"
      end

    label = if outcome == :cancellation, do: "cancellation", else: "execution timeout"

    %{
      "status" => "failed",
      "result" => nil,
      "error" =>
        Observability.sanitize_error(
          "dagster_run #{label} could not confirm process exit; #{signal_detail}"
        ),
      "exit_code" => nil,
      "logs_tail" => sanitized_logs_tail(logs_tail),
      "failure_category" => "#{outcome}_termination_unconfirmed"
    }
  end

  defp dagster_run_timeout_ms(params) do
    value =
      Map.get(params, "execution_timeout_seconds") ||
        System.get_env(
          "DISPATCH_DAGSTER_RUN_TIMEOUT_SECONDS",
          Integer.to_string(@default_dagster_run_timeout_seconds)
        )

    case parse_positive_integer(value) do
      {:ok, seconds} -> {:ok, seconds * 1_000}
      :error -> {:error, "execution_timeout_seconds must be a positive integer"}
    end
  end

  defp parse_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp parse_positive_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _ -> :error
    end
  end

  defp parse_positive_integer(_value), do: :error

  defp append_tail(logs_tail, data) do
    combined = logs_tail <> data

    if byte_size(combined) > @logs_tail_bytes do
      binary_part(combined, byte_size(combined) - @logs_tail_bytes, @logs_tail_bytes)
    else
      combined
    end
  end

  defp execute_script(python_root, script_path, params) do
    case System.cmd(
           python_bin(),
           ["-c", @runner, script_path, Jason.encode!(params)],
           cd: python_root,
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        with {:ok, response} <- Jason.decode(output) do
          format_response(response)
        else
          {:error, reason} ->
            failed_result("invalid python runner output: #{inspect(reason)}")
        end

      {output, exit_code} ->
        failed_result("python runner exited with code #{exit_code}: #{String.trim(output)}")
    end
  end

  defp format_response(%{"returncode" => 0, "stdout" => stdout, "stderr" => stderr}) do
    %{
      "status" => "success",
      "result" => empty_to_nil(stdout),
      "error" => empty_to_nil(stderr)
    }
  end

  defp format_response(%{"stdout" => stdout, "stderr" => stderr}) do
    %{
      "status" => "failed",
      "result" => empty_to_nil(stdout),
      "error" => empty_to_nil(stderr) || "python script failed"
    }
  end

  defp failed_result(error, failure_category \\ "executor_error") do
    %{
      "status" => "failed",
      "result" => nil,
      "error" => Observability.sanitize_error(error),
      "failure_category" => failure_category
    }
  end

  defp sanitized_logs_tail(value) do
    value
    |> empty_to_nil()
    |> Observability.sanitize_logs_tail()
  end

  defp process_group_command(executable, args) do
    case :os.type() do
      {:unix, _name} ->
        with setsid when is_binary(setsid) <- System.find_executable("setsid"),
             kill when is_binary(kill) <- System.find_executable("kill"),
             pgrep when is_binary(pgrep) <- System.find_executable("pgrep") do
          {:ok, {setsid, ["--fork", "--wait", "--", executable | args]}}
        else
          nil ->
            {:error,
             "setsid, kill, and pgrep are required for process-group-aware Dagster execution on Unix"}
        end

      _ ->
        {:ok, {executable, args}}
    end
  end

  defp empty_to_nil(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp python_root do
    System.get_env("PYTHON_ROOT", "../python")
    |> Path.expand(File.cwd!())
  end

  defp python_bin do
    System.get_env("PYTHON_BIN") ||
      System.find_executable("python") ||
      System.find_executable("python3") ||
      "python"
  end
end
