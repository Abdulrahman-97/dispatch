defmodule Dispatch.RateLimit do
  @moduledoc false

  @key_pattern ~r/^[A-Za-z0-9_.:-]+$/

  def spec_from_params(params) when is_map(params) do
    case Map.get(params, "rate_limit_key") do
      nil ->
        :none

      "" ->
        :none

      key ->
        with {:ok, normalized_key} <- normalize_key(key),
             {:ok, cost} <- normalize_cost(Map.get(params, "rate_limit_cost", 1)) do
          {:ok, %{key: normalized_key, cost: cost}}
        end
    end
  end

  def spec_from_params(_params), do: :none

  def metadata_from_params(params) do
    case spec_from_params(params) do
      :none ->
        {:ok,
         %{
           "rate_limit_key" => "",
           "rate_limit_cost" => "",
           "rate_limit_wait_ms" => ""
         }}

      {:ok, %{key: key, cost: cost}} ->
        {:ok,
         %{
           "rate_limit_key" => key,
           "rate_limit_cost" => Integer.to_string(cost),
           "rate_limit_wait_ms" => "0"
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

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
