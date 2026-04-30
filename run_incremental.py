#!/usr/bin/env python3
"""
Runs all Metorik incremental API pulls in sequence.
Entrypoint for Cloud Run Job.
"""
import subprocess, sys

scripts = [
    ["python", "metorik_orders_api.py",    "--mode", "incremental"],
    ["python", "metorik_customers_api.py", "--mode", "incremental"],
    ["python", "metorik_ad_spend.py",      "--mode", "incremental"],
    ["python", "metorik_profit.py",        "--mode", "incremental"],
    ["python", "metorik_revenue.py",       "--mode", "incremental"],
    ["python", "metorik_sources_utms.py"],
]

for cmd in scripts:
    print(f"\n▶ {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Failed: {' '.join(cmd)}")
        sys.exit(result.returncode)

print("\n✅ All Metorik scripts complete")
