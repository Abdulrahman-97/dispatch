defmodule Dispatch.UUID do
  @moduledoc false

  import Bitwise

  def generate do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    version = bor(band(c, 0x0FFF), 0x4000)
    variant = bor(band(d, 0x3FFF), 0x8000)

    :io_lib.format(
      "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
      [a, b, version, variant, e]
    )
    |> IO.iodata_to_binary()
  end
end
