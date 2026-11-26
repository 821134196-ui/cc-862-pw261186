#!/usr/bin/env python3
"""Launcher for the monitoring dashboard: python3 run_dashboard.py [port]."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web.app import serve  # noqa: E402

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    serve(host="127.0.0.1", port=port)
