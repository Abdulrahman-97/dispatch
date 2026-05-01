defmodule Dispatch.RateLimit do
  @moduledoc false

  @key_pattern ~r/^[A-Za-z0-9_.:-]+$/

  def specs_from_params(params) when is_map(params) do
    case Map.get(params, "rate_limits") do
      nil -> legacy_specs_from_params(params)
      rate_limits -> normalize_rate_limits(rate_limits)
    end
  end

  def specs_from_params(_params), do: {:ok, []}

  def spec_from_params(params) when is_map(params) do
    case specs_from_params(params) do
      {:ok, []} -> :none
      {:ok, [spec]} -> {:ok, spec}
      {:ok, _specs} -> {:error, "multiple rate limits require params.rate_limits"}
      {:error, reason} -> {:error, reason}
    end
  end

  def spec_from_params(_params), do: :none

  def metadata_from_params(params) when is_map(params) do
    case specs_from_params(params) do
      {:ok, specs} ->
        rate_limits = Map.new(specs, fn %{key: key, cost: cost} -> {key, cost} end)
        legacy = if length(specs) == 1, do: hd(specs), else: nil

        {:ok,
         %{
           "rate_limit_key" => if(legacy, do: legacy.key, else: ""),
           "rate_limit_cost" => if(legacy, do: Integer.to_string(legacy.cost), else: ""),
           "rate_limits" =>
             if(map_size(rate_limits) == 0, do: "", else: Jason.encode!(rate_limits)),
           "rate_limit_wait_ms" => "0"
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def metadata_from_params(_params), do: metadata_from_params(%{})

  defp legacy_specs_from_params(params) do
    case Map.get(params, "rate_limit_key") do
      nil ->
        {:ok, []}

      "" ->
        {:ok, []}

      key ->
        with {:ok, normalized_key} <- normalize_key(key),
             {:ok, cost} <- normalize_cost(Map.get(params, "rate_limit_cost", 1)) do
          {:ok, [%{key: normalized_key, cost: cost}]}
        end
    end
  end

  defp normalize_rate_limits(rate_limits) when is_map(rate_limits) do
    Enum.reduce_while(rate_limits, {:ok, []}, fn {key, cost}, {:ok, acc} ->
      with {:ok, normalized_key} <- normalize_key(key),
           {:ok, normalized_cost} <- normalize_cost(cost) do
        {:cont, {:ok, [%{key: normalized_key, cost: normalized_cost} | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, specs} -> {:ok, Enum.reverse(specs)}
      other -> other
    end
  end

  defp normalize_rate_limits(_rate_limits), do: {:error, "rate_limits must be a JSON object"}

  def normalize_key(value) when is_binary(value) do
    key = String.trim(value)

    cond do
      key == "" ->
        {:error, "rate_limit_key must be a non-empty string"}

      not String.match?(key, @key_pattern) ->
        {:error, "rate_limit_key may only contain letters, numbers, _, ., :, or -"}

      true ->
        {:ok, key}
    end
  end

  def normalize_key(_value), do: {:error, "rate_limit_key must be a string"}

  def normalize_cost(value) when is_integer(value) and value > 0, do: {:ok, value}

  def normalize_cost(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _ -> {:error, "rate_limit_cost must be a positive integer"}
    end
  end

  def normalize_cost(_value), do: {:error, "rate_limit_cost must be a positive integer"}
end
