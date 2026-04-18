from __future__ import annotations

import json
import sys
from datetime import UTC, datetime


def main() -> int:
    if len(sys.argv) < 2:
        sys.stderr.write("missing params JSON\n")
        return 1

    raw_params = sys.argv[1]

    try:
        params = json.loads(raw_params)
    except json.JSONDecodeError:
        sys.stderr.write("params must be valid JSON\n")
        return 1

    if not isinstance(params, dict):
        sys.stderr.write("params must be a JSON object\n")
        return 1

    symbol_value = params.get("symbol")

    if not isinstance(symbol_value, str) or not symbol_value.strip():
        sys.stderr.write("symbol must be a non-empty string\n")
        return 1

    symbol = symbol_value.strip().upper()

    if symbol == "FAIL":
        sys.stderr.write("intentional failure for testing\n")
        return 1

    payload = {
        "symbol": symbol,
        "price": demo_price(symbol),
        "fetched_at": datetime.now(UTC).isoformat(),
    }

    sys.stdout.write(json.dumps(payload))
    return 0


def demo_price(symbol: str) -> float:
    return round(sum(ord(character) for character in symbol) / 10, 2)


if __name__ == "__main__":
    raise SystemExit(main())
