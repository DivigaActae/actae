# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import os

ACTAE_BASE_URL = os.getenv("ACTAE_BASE_URL", "http://localhost:8002")
ACTAE_WS_URL = os.getenv("ACTAE_WS_URL", "ws://localhost:8002")
ACTAE_API_URL = os.getenv("ACTAE_API_URL", "https://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "")

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "9090"))
