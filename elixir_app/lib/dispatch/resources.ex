defmodule Dispatch.Resources do
  @moduledoc false

  @resource_key_pattern ~r/^[A-Za-z0-9_.:-]+$/

  def worker_capacity_from_env(default_slots) do
    case System.get_env("DISPATCH_WORKER_RESOURCES_JSON") do
      nil -> {:ok, %{"default_slots" => default_slots}}
      "" -> {:ok, %{"default_slots" => default_slots}}
      raw -> parse_resource_map_json(raw)
    end
  end

  def requirements_from_params(params) when is_map(params) do
    case Map.get(params, "resources") do
      nil -> {:ok, %{"default_slots" => 1}}
      resources -> normalize_resource_map(resources)
    end
  end

  def requirements_from_params(_params), do: {:ok, %{"default_slots" => 1}}

  def normalize_resource_map(resources) when is_map(resources) do
    if map_size(resources) == 0 do
      {:error, "resources must not be empty"}
    else
      Enum.reduce_while(resources, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        with {:ok, normalized_key} <- normalize_resource_key(key),
             {:ok, amount} <- normalize_positive_integer(value, "#{normalized_key} resource") do
          {:cont, {:ok, Map.put(acc, normalized_key, amount)}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  def normalize_resource_map(_resources), do: {:error, "resources must be a JSON object"}

  def normalize_available_resource_map(resources) when is_map(resources) do
    if map_size(resources) == 0 do
      {:error, "available_resources must not be empty"}
    else
      Enum.reduce_while(resources, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        with {:ok, normalized_key} <- normalize_resource_key(key),
             {:ok, amount} <- normalize_non_negative_integer(value, "#{normalized_key} resource") do
          {:cont, {:ok, Map.put(acc, normalized_key, amount)}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  def normalize_available_resource_map(_resources) do
    {:error, "available_resources must be a JSON object"}
  end

  def parse_resource_map_json(raw) do
    with {:ok, decoded} <- Jason.decode(raw),
         {:ok, resources} <- normalize_resource_map(decoded) do
      {:ok, resources}
    else
      {:error, %Jason.DecodeError{}} -> {:error, "resources JSON must be valid JSON"}
      {:error, reason} -> {:error, reason}
    end
  end

  def fits?(requirements, available) do
    Enum.all?(requirements, fn {key, amount} -> Map.get(available, key, 0) >= amount end)
  end

  def missing_keys(requirements, capacity) do
    requirements
    |> Map.keys()
    |> Enum.reject(&Map.has_key?(capacity, &1))
  end

  def subtract(capacity, requirements) do
    Map.new(capacity, fn {key, value} -> {key, value - Map.get(requirements, key, 0)} end)
  end

  def add(available, released, capacity) do
    Map.new(capacity, fn {key, max_value} ->
      {key, min(max_value, Map.get(available, key, 0) + Map.get(released, key, 0))}
    end)
  end

  def encode(resources), do: Jason.encode!(resources)

  def decode(nil), do: nil
  def decode(""), do: nil

  def decode(raw) when is_binary(raw) do
    case Jason.decode(raw) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _ -> nil
    end
  end

  def decode(_raw), do: nil

  defp normalize_resource_key(value) when is_binary(value) do
    key = String.trim(value)

    cond do
      key == "" ->
        {:error, "resource key must be a non-empty string"}

      not String.match?(key, @resource_key_pattern) ->
        {:error, "resource key may only contain letters, numbers, _, ., :, or -"}

      true ->
        {:ok, key}
    end
  end

  defp normalize_resource_key(_value), do: {:error, "resource key must be a string"}

  defp normalize_positive_integer(value, _label) when is_integer(value) and value > 0 do
    {:ok, value}
  end

  defp normalize_positive_integer(value, label) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _ -> {:error, "#{label} must be a positive integer"}
    end
  end

  defp normalize_positive_integer(_value, label),
    do: {:error, "#{label} must be a positive integer"}

  defp normalize_non_negative_integer(value, _label) when is_integer(value) and value >= 0 do
    {:ok, value}
  end

  defp normalize_non_negative_integer(value, label) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _ -> {:error, "#{label} must be a non-negative integer"}
    end
  end

  defp normalize_non_negative_integer(_value, label),
    do: {:error, "#{label} must be a non-negative integer"}
end
