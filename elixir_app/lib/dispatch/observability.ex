defmodule Dispatch.Observability do
  @moduledoc false

  require Logger

  @default_text_limit 4_096
  @logs_tail_limit 16_384
  @collection_limit 50
  @sensitive_key ~r/(api[_-]?key|authorization|credential|password|secret|token)/i
  @assignment_secret ~r/\b(api[_-]?key|authorization|credential|password|secret|token)\b\s*[:=]\s*[^\s,;]+/i
  @bearer_secret ~r/\bBearer\s+[^\s,;]+/i
  @url_userinfo ~r{(https?://)[^/\s:@]+:[^@\s/]+@}i

  def event(event, attrs \\ %{}, level \\ :info)
      when is_binary(event) and is_map(attrs) do
    payload =
      attrs
      |> sanitize_map()
      |> Map.put("event", event)
      |> Map.put_new("timestamp", now_iso8601())

    Logger.log(level, Jason.encode!(payload))
  end

  def sanitize_error(value), do: sanitize_text(value, @default_text_limit)
  def sanitize_logs_tail(value), do: sanitize_text(value, @logs_tail_limit)

  def sanitize_text(nil, _limit), do: nil

  def sanitize_text(value, limit) when is_integer(limit) and limit > 0 do
    value
    |> to_string()
    |> redact_secrets()
    |> truncate(limit)
  end

  def sanitize_map(value) when is_map(value) do
    value
    |> Enum.take(@collection_limit)
    |> Map.new(fn {key, item} ->
      normalized_key = to_string(key)

      if String.match?(normalized_key, @sensitive_key) do
        {normalized_key, "[REDACTED]"}
      else
        {normalized_key, sanitize_value(item)}
      end
    end)
  end

  def sanitize_map(_value), do: %{}

  defp sanitize_value(value) when is_map(value), do: sanitize_map(value)

  defp sanitize_value(value) when is_list(value) do
    value
    |> Enum.take(@collection_limit)
    |> Enum.map(&sanitize_value/1)
  end

  defp sanitize_value(value) when is_binary(value), do: sanitize_text(value, @default_text_limit)

  defp sanitize_value(value) when is_number(value) or is_boolean(value) or is_nil(value),
    do: value

  defp sanitize_value(value), do: sanitize_text(inspect(value), @default_text_limit)

  defp redact_secrets(value) do
    value
    |> then(&Regex.replace(@assignment_secret, &1, "\\1=[REDACTED]"))
    |> then(&Regex.replace(@bearer_secret, &1, "Bearer [REDACTED]"))
    |> then(&Regex.replace(@url_userinfo, &1, "\\1[REDACTED]@"))
  end

  defp truncate(value, limit) do
    if String.length(value) <= limit do
      value
    else
      String.slice(value, 0, limit) <> "...[truncated]"
    end
  end

  defp now_iso8601 do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
