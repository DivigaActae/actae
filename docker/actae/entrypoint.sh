#!/bin/sh
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

# Actae container entrypoint.
#
# The compose stack bind-mounts the dev PKI read-only (./certs:/certs:ro);
# the private keys are mode 600 and owned by the host user, so the unprivileged
# runtime user cannot read them directly. Copy the PKI into an app-owned
# location, then drop privileges and exec Actae.
#
# In deployments without a certs mount the copy is skipped and the app uses
# its configured (env-provided) paths as-is.
set -eu

if [ -d /certs ] && [ -f /certs/cert.pem ]; then
    mkdir -p /app/certs
    cp -a /certs/. /app/certs/
    chown -R actae:actae /app/certs
fi

exec setpriv --reuid=1001 --regid=1001 --clear-groups /usr/local/bin/actae "$@"
