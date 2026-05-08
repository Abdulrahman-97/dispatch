defmodule Dispatch.Worker.Executor do
  @moduledoc false

  @dagster_cancel_check_interval_ms 500
  @dagster_cancel_timeout_ms 5_000
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
         env when is_map(env) <- Map.get(params, "env", %{}) do
      execute_dagster_command(executable, args, env, opts)
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
    env = Enum.map(env, fn {key, value} -> {to_charlist(key), to_charlist(to_string(value))} end)

    port =
      Port.open({:spawn_executable, resolved_executable}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args,
        env: env
      ])

    wait_for_port(port, command, opts, "")
  end

  defp wait_for_port(port, command, opts, logs_tail) do
    cancel_check = Keyword.get(opts, :cancel_check, fn -> false end)

    cancel_interval_ms =
      Keyword.get(opts, :cancel_check_interval_ms, @dagster_cancel_check_interval_ms)

    receive do
      {^port, {:data, data}} ->
        wait_for_port(port, command, opts, append_tail(logs_tail, data))

      {^port, {:exit_status, exit_code}} ->
        dagster_result(exit_code, logs_tail)
    after
      cancel_interval_ms ->
        if cancel_check.() do
          cancel_port(port, command, opts, logs_tail)
        else
          wait_for_port(port, command, opts, logs_tail)
        end
    end
  end

  defp cancel_port(port, command, opts, logs_tail) do
    timeout_ms = Keyword.get(opts, :cancel_timeout_ms, @dagster_cancel_timeout_ms)
    os_pid = port_os_pid(port)

    terminate_os_process(os_pid, :graceful)
    wait_for_canceled_port(port, command, os_pid, timeout_ms, logs_tail)
  end

  defp wait_for_canceled_port(port, command, os_pid, timeout_ms, logs_tail) do
    receive do
      {^port, {:data, data}} ->
        wait_for_canceled_port(port, command, os_pid, timeout_ms, append_tail(logs_tail, data))

      {^port, {:exit_status, exit_code}} ->
        canceled_result(exit_code, logs_tail)
    after
      timeout_ms ->
        terminate_os_process(os_pid, :force)
        canceled_result(nil, logs_tail)
    end
  end

  defp port_os_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when is_integer(pid) -> pid
      _ -> nil
    end
  end

  defp terminate_os_process(nil, _mode), do: :ok

  defp terminate_os_process(pid, mode) do
    if match?({:win32, _name}, :os.type()) do
      args =
        case mode do
          :force -> ["/PID", Integer.to_string(pid), "/T", "/F"]
          :graceful -> ["/PID", Integer.to_string(pid), "/T"]
        end

      System.cmd("taskkill", args, stderr_to_stdout: true)
    else
      signal = if mode == :force, do: "-KILL", else: "-TERM"
      System.cmd("kill", [signal, Integer.to_string(pid)], stderr_to_stdout: true)
    end

    :ok
  rescue
    _exception -> :ok
  end

  defp dagster_result(0, logs_tail) do
    %{
      "status" => "success",
      "result" => nil,
      "error" => nil,
      "exit_code" => 0,
      "logs_tail" => empty_to_nil(logs_tail)
    }
  end

  defp dagster_result(exit_code, logs_tail) do
    %{
      "status" => "failed",
      "result" => nil,
      "error" => "dagster_run command exited with code #{exit_code}",
      "exit_code" => exit_code,
      "logs_tail" => empty_to_nil(logs_tail)
    }
  end

  defp canceled_result(exit_code, logs_tail) do
    %{
      "status" => "canceled",
      "result" => nil,
      "error" => "dagster_run canceled",
      "exit_code" => exit_code,
      "logs_tail" => empty_to_nil(logs_tail)
    }
  end

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

  defp failed_result(error) do
    %{
      "status" => "failed",
      "result" => nil,
      "error" => error
    }
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
