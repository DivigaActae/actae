#!/usr/bin/env bash
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

#
# Regenerate the self-signed PKI used by the Actae docker stack and the
# integration test suite.
#
# Produces:
#   certs/ca.crt                          — CA certificate (trusted by clients/tests)
#   certs/cert.pem + certs/key.pem        — Actae server certificate (TLS)
#   certs/client/actae-test-client.{crt,key} — mTLS client cert for tests/CI
#
# Idempotent: re-running rotates the server + client certs with a fresh CA.
# Restart the stack after rotating:
#
#   ./scripts/rotate-certs.sh && docker compose up -d --force-recreate actae
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$ROOT/certs"
CLIENT_DIR="$CERT_DIR/client"

mkdir -p "$CERT_DIR" "$CLIENT_DIR"

# ---------------------------------------------------------------------------
# 1. Certificate authority
# ---------------------------------------------------------------------------
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -days 3650 \
    -subj "/CN=Actae Local CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# ---------------------------------------------------------------------------
# 2. Server certificate (localhost + 127.0.0.1 SANs)
# ---------------------------------------------------------------------------
openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERT_DIR/key.pem" \
    -out "$CERT_DIR/server.csr" \
    -subj "/CN=localhost" 2>/dev/null

openssl x509 -req -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/cert.pem" -days 825 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth") 2>/dev/null

rm -f "$CERT_DIR/server.csr"

# ---------------------------------------------------------------------------
# 3. mTLS client CA + test client certificate
# ---------------------------------------------------------------------------
# The server validates client certs against this CA (CA_CERT_PATH in compose).
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$CLIENT_DIR/client-ca.key" \
    -out "$CLIENT_DIR/client-ca.crt" \
    -days 3650 \
    -subj "/CN=Actae Client CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

openssl req -newkey rsa:2048 -nodes \
    -keyout "$CLIENT_DIR/actae-test-client.key" \
    -out "$CLIENT_DIR/actae-test-client.csr" \
    -subj "/CN=actae-test-client" 2>/dev/null

openssl x509 -req -in "$CLIENT_DIR/actae-test-client.csr" \
    -CA "$CLIENT_DIR/client-ca.crt" -CAkey "$CLIENT_DIR/client-ca.key" -CAcreateserial \
    -out "$CLIENT_DIR/actae-test-client.crt" -days 825 \
    -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth") 2>/dev/null

rm -f "$CLIENT_DIR/actae-test-client.csr"

chmod 600 "$CERT_DIR/ca.key" "$CERT_DIR/key.pem" "$CLIENT_DIR/client-ca.key" "$CLIENT_DIR/actae-test-client.key"

echo "PKI regenerated:"
echo "  CA:               $CERT_DIR/ca.crt"
echo "  Server cert:      $CERT_DIR/cert.pem"
echo "  Server key:       $CERT_DIR/key.pem"
echo "  Client CA:        $CLIENT_DIR/client-ca.crt"
echo "  Test client cert: $CLIENT_DIR/actae-test-client.crt"
echo
echo "Restart the stack to pick up the new certs:"
echo "  docker compose up -d --force-recreate actae"
