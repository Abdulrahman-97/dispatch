defmodule Dispatch.Coordinator.DagsterRun do
  @moduledoc false

  def validate_params(params) when is_map(params) do
    with {:ok, dagster_run_id} <- required_string(params, "dagster_run_id"),
         {:ok, command} <- command(params["command"]),
         {:ok, env} <- env(Map.get(params, "env", %{})),
         {:ok, image} <- optional_string(params, "image"),
         {:ok, metadata} <- metadata(Map.get(params, "metadata", %{})) do
      {:ok,
       Map.merge(params, %{
         "dagster_run_id" => dagster_run_id,
         "command" => command,
         "env" => env,
         "image" => image || "",
         "metadata" => metadata
       })}
    end
  end

  def validate_params(_params), do: {:error, "params must be a JSON object"}

  def attrs(params) do
    %{
      "dagster_run_id" => params["dagster_run_id"],
      "idempotency_key" => params["dagster_run_id"],
      "command" => Jason.encode!(params["command"]),
      "image" => params["image"] || "",
      "metadata" => Jason.encode!(params["metadata"] || %{})
    }
  end

  defp required_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, "#{key} is required"}, else: {:ok, value}

      _value ->
        {:error, "#{key} is required"}
    end
  end

  defp optional_string(params, key) do
    case Map.get(params, key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, String.trim(value)}
      _value -> {:error, "#{key} must be a string"}
    end
  end

  defp command([executable | _args] = command)
       when is_binary(executable) and executable != "" do
    if Enum.all?(command, &is_binary/1) do
      {:ok, command}
    else
      {:error, "command must be a non-empty array of strings"}
    end
  end

  defp command(_command), do: {:error, "command must be a non-empty array of strings"}

  defp env(env) when is_map(env) do
    env
    |> Enum.reduce_while({:ok, %{}}, fn {key, value}, {:ok, acc} ->
      cond do
        not is_binary(key) or String.trim(key) == "" ->
          {:halt, {:error, "env keys must be non-empty strings"}}

        is_nil(value) ->
          {:cont, {:ok, Map.put(acc, key, "")}}

        is_binary(value) or is_number(value) or is_boolean(value) ->
          {:cont, {:ok, Map.put(acc, key, to_string(value))}}

        true ->
          {:halt, {:error, "env values must be strings, numbers, booleans, or null"}}
      end
    end)
  end

  defp env(_env), do: {:error, "env must be a JSON object"}

  defp metadata(metadata) when is_map(metadata), do: {:ok, metadata}
  defp metadata(_metadata), do: {:error, "metadata must be a JSON object"}
end
