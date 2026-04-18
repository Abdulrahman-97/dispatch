defmodule Dispatch.Worker.Executor do
  @moduledoc false

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

  def run(%{"job_type" => job_type, "params" => params}) do
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
