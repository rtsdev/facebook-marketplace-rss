FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /app

ENV CONFIG_FILE="./config/config.json" \
    SAMPLE_CONFIG_FILE="config.sample.json"

COPY . .

RUN uv sync && \
    uv run playwright install --with-deps --only-shell

EXPOSE 5000

ENTRYPOINT ["./entrypoint.sh"]
