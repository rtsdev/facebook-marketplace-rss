#!/usr/bin/env sh

if [ -f "$CONFIG_FILE" ]; then
  true
else
    cp config.sample.json "$CONFIG_FILE"
fi

uv run fb_ad_monitor.py