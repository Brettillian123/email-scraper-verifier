"""
Script to find the correct preflight function name in smtp.py
Run this in your EMAIL-SCRAPER directory:
  python find_preflight.py
"""

import re
from pathlib import Path

smtp_file = Path("src/verify/smtp.py")

if not smtp_file.exists():
    print("❌ src/verify/smtp.py not found!")
    exit(1)

content = smtp_file.read_text(encoding="utf-8")

# Search for preflight-related functions
print("Searching for preflight-related code in smtp.py:\n")
print("=" * 60)

# Find all function definitions
func_pattern = r"^def (_?\w*preflight\w*)\s*\("
for i, line in enumerate(content.split("\n"), 1):
    if match := re.search(func_pattern, line, re.IGNORECASE):
        func_name = match.group(1)
        print(f"Line {i}: {func_name}")
        print(f"  {line.strip()}")
        print()

# Search for preflight calls
print("\n" + "=" * 60)
print("Preflight function CALLS in probe_rcpt:\n")

call_pattern = r"(\w*preflight\w*)\s*\("
in_probe_rcpt = False
for i, line in enumerate(content.split("\n"), 1):
    if "def probe_rcpt(" in line:
        in_probe_rcpt = True
    elif in_probe_rcpt and line.startswith("def "):
        in_probe_rcpt = False

    if in_probe_rcpt:
        if match := re.search(call_pattern, line, re.IGNORECASE):
            func_name = match.group(1)
            if "preflight" in func_name.lower():
                print(f"Line {i}: {line.strip()}")

print("\n" + "=" * 60)
print("\nTo fix the test, you need to patch the function found above.")
print("The function is likely imported from src.verify.preflight")
