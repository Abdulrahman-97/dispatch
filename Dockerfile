FROM elixir:1.19.1-otp-28

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    python3 \
    python3-venv \
    python-is-python3 \
  && rm -rf /var/lib/apt/lists/*

ARG PYTHON_PACKAGE_SPEC=""

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV MIX_ENV=prod
ENV VIRTUAL_ENV=/opt/dispatch-python
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PYTHON_BIN="${VIRTUAL_ENV}/bin/python"

RUN python3 -m venv "${VIRTUAL_ENV}" \
  && "${VIRTUAL_ENV}/bin/python" -m pip install --upgrade pip setuptools wheel \
  && if [ -n "${PYTHON_PACKAGE_SPEC}" ]; then \
    "${VIRTUAL_ENV}/bin/python" -m pip install "${PYTHON_PACKAGE_SPEC}"; \
  fi

WORKDIR /app/elixir_app

RUN mix local.hex --force && mix local.rebar --force

COPY elixir_app/mix.exs elixir_app/mix.lock ./

RUN mix deps.get --only prod
RUN mix deps.compile

COPY elixir_app/ ./
COPY python/ /app/python/

RUN mix compile

EXPOSE 4000

CMD ["mix", "run", "--no-halt"]
