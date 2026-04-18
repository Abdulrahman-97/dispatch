FROM elixir:1.19.1-otp-28

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    python3 \
    python-is-python3 \
  && rm -rf /var/lib/apt/lists/*

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV MIX_ENV=prod

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

