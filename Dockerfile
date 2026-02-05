FROM ghcr.io/astral-sh/uv:debian

WORKDIR /app

RUN mkdir -m 700 ~/.gnupg && \
    install -d -m 0755 /etc/apt/keyrings && \
    wget -q https://packages.mozilla.org/apt/repo-signing-key.gpg -O- | tee /etc/apt/keyrings/packages.mozilla.org.asc > /dev/null && \
    gpg -n -q --import --import-options import-show /etc/apt/keyrings/packages.mozilla.org.asc | awk '/pub/{getline; gsub(/^ +| +$/,""); if($0 == "35BAA0B33E9EB396F59CA838C0BA5CE6DC6315A3") { print "\nThe key fingerprint matches ("$0").\n"; exit 0 } else { print "\nVerification failed: the fingerprint ("$0") does not match the expected one.\n"; exit 1 }}' && \
    echo "deb [signed-by=/etc/apt/keyrings/packages.mozilla.org.asc] https://packages.mozilla.org/apt mozilla main" | tee -a /etc/apt/sources.list.d/mozilla.list > /dev/null && \
    echo 'Package: *\nPin: origin packages.mozilla.org\nPin-Priority: 1000\n' | tee /etc/apt/preferences.d/mozilla && \
    apt-get update && \
    apt-get install -y firefox && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m -s /bin/bash -u 501 appuser && \
    chown appuser:appuser .

COPY --chown=appuser:appuser . .

USER appuser

RUN uv sync

EXPOSE 5000

CMD ["uv", "run", "fb_ad_monitor.py"]
