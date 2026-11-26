#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Issue a client certificate for an operator, agent, or CI runner.
#
# Actae enforces mTLS: every client that connects (HTTP or WebSocket) must
# present a certificate signed by the client CA (certs/client/client-ca.crt).
# This script mints such a certificate:
#
#   ./scripts/issue-client-cert.sh deploy-agent
#   # → certs/client/deploy-agent.crt + certs/client/deploy-agent.key
#
# Modes:
#   (default)   print the connect snippets for the issued identity
#   --install   also drop the identity into ~/.actae/ (ca.crt, client.crt,
#               client.key) so the Python/Go SDKs pick it up automatically
#               with zero certificate configuration
#   --bundle F  package ca.crt + the identity into a tar.gz for transfer
#               to a remote client (extract it into ~/.actae/ over there)
#
# Distribute the .crt/.key pair (and certs/ca.crt) to the client; never
# share the CA private keys. The server validates against the client CA at
# startup (CA_CERT_PATH in docker-compose.yml), so a newly issued cert works
# without a server restart.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIENT_DIR="$ROOT/certs"

NAME="${1:-}"
INSTALL=0
BUNDLE=""

shift || true
while [ $# -gt 0 ]; do
    case "$1" in
        --install) INSTALL=1 ;;
        --bundle)
            shift
            BUNDLE="${1:-}"
            [ -n "$BUNDLE" ] || { echo "--bundle requires a file path" >&2; exit 1; }
            ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
    shift || true
done

if [ -z "$NAME" ]; then
    echo "usage: $0 <client-name> [--install] [--bundle <file.tar.gz>]" >&2
    exit 1
fi

mkdir -p "$CLIENT_DIR"

# The client CA must exist — generate it first if missing.
if [ ! -f "$CLIENT_DIR/client/client-ca.key" ] || [ ! -f "$CLIENT_DIR/client/client-ca.crt" ]; then
    echo "Client CA missing — run ./scripts/rotate-certs.sh first" >&2
    exit 1
fi

CERT="$CLIENT_DIR/client/$NAME.crt"
KEY="$CLIENT_DIR/client/$NAME.key"

openssl req -newkey rsa:2048 -nodes \
    -keyout "$KEY" \
    -out "$CLIENT_DIR/client/$NAME.csr" \
    -subj "/CN=$NAME" 2>/dev/null

openssl x509 -req -in "$CLIENT_DIR/client/$NAME.csr" \
    -CA "$CLIENT_DIR/client/client-ca.crt" -CAkey "$CLIENT_DIR/client/client-ca.key" -CAcreateserial \
    -out "$CERT" -days 825 \
    -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth") 2>/dev/null

rm -f "$CLIENT_DIR/client/$NAME.csr"
chmod 600 "$KEY"

echo "Client certificate issued for '$NAME':"
echo "  cert: $CERT"
echo "  key:  $KEY"
echo
echo "Connect with curl:"
echo "  curl --cacert certs/ca.crt --cert $CERT --key $KEY https://localhost:8002/healthz"
echo
echo "Connect with the Python SDK:"
echo "  ActaeClient(endpoint='https://localhost:8002', api_key='sk-...',"
echo "              ca_cert='certs/ca.crt', client_cert='$CERT', client_key='$KEY')"

if [ "$INSTALL" -eq 1 ]; then
    CONFIG_DIR="${ACTAE_CONFIG_DIR:-$HOME/.actae}"
    mkdir -p "$CONFIG_DIR"
    cp "$CLIENT_DIR/ca.crt" "$CONFIG_DIR/ca.crt"
    cp "$CERT" "$CONFIG_DIR/client.crt"
    cp "$KEY" "$CONFIG_DIR/client.key"
    echo
    echo "Installed default identity into $CONFIG_DIR — the SDKs now connect"
    echo "to TLS instances with zero certificate configuration."
fi

if [ -n "$BUNDLE" ]; then
    TMP="$(mktemp -d)"
    cp "$CLIENT_DIR/ca.crt" "$TMP/ca.crt"
    cp "$CERT" "$TMP/client.crt"
    cp "$KEY" "$TMP/client.key"
    tar -C "$TMP" -czf "$BUNDLE" ca.crt client.crt client.key
    rm -rf "$TMP"
    echo
    echo "Bundle written to $BUNDLE — on the client machine:"
    echo "  mkdir -p ~/.actae && tar -xzf $BUNDLE -C ~/.actae"
fi
