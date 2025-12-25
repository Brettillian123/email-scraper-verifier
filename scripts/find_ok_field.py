"""
Find the exact line in smtp.py where the "ok" field is set incorrectly.
Run: python find_ok_field.py
"""

from pathlib import Path

smtp_file = Path("src/verify/smtp.py")
if not smtp_file.exists():
    print("❌ src/verify/smtp.py not found!")
    exit(1)

content = smtp_file.read_text(encoding="utf-8")
lines = content.split("\n")

print("\n" + "=" * 70)
print("SEARCHING FOR 'ok' FIELD IN RETURN STATEMENTS")
print("=" * 70 + "\n")

in_probe_rcpt = False
found_any = False

for i, line in enumerate(lines, 1):
    # Track if we're in probe_rcpt function
    if "def probe_rcpt(" in line:
        in_probe_rcpt = True
        print(f"📍 Found probe_rcpt at line {i}\n")
    elif in_probe_rcpt and line.strip().startswith("def ") and "probe_rcpt" not in line:
        in_probe_rcpt = False

    # Look for "ok" field in return statements within probe_rcpt
    if in_probe_rcpt and '"ok"' in line:
        found_any = True
        print(f"Line {i}: {line}")

        # Show context (2 lines before and after)
        start = max(0, i - 3)
        end = min(len(lines), i + 3)
        print("\nContext:")
        for j in range(start, end):
            marker = ">>> " if j == i - 1 else "    "
            print(f"{marker}{j + 1:4d}: {lines[j]}")
        print("\n" + "-" * 70 + "\n")

if not found_any:
    print("❌ Could not find 'ok' field in probe_rcpt")
    print("\nSearching for all return statements in probe_rcpt...")

    in_probe_rcpt = False
    for i, line in enumerate(lines, 1):
        if "def probe_rcpt(" in line:
            in_probe_rcpt = True
        elif in_probe_rcpt and line.strip().startswith("def "):
            in_probe_rcpt = False

        if in_probe_rcpt and "return" in line and "{" in line:
            print(f"Line {i}: {line}")

print("\n" + "=" * 70)
print("FIX INSTRUCTIONS:")
print("=" * 70)
print("""
Change the line with "ok": ... to:

    "ok": category == "accept" and error_str is None,

This ensures ok=True only for 2xx accept responses, not for 5xx rejections.
""")
