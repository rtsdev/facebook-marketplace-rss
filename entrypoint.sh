#!/usr/bin/env sh

if [ -f "$CONFIG_FILE" ]; then
  echo "Config file already exists at $CONFIG_FILE"
  true
else
  echo "Config file not found. Copying $SAMPLE_CONFIG_FILE to $CONFIG_FILE"
    cp "$SAMPLE_CONFIG_FILE" "$CONFIG_FILE"
fi

uv run fb_ad_monitor.py
