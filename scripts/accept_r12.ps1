# R12 acceptance: generate + verify enqueue smoke
Set-Location $PSScriptRoot\..
python -m scripts.generate_permutations --first Brett --last Anderson --domain crestwellpartners.com
# Expect several permutations printed; use one to spot-check that your verify enqueue path works (R15+ wires).
# (Reminder: in PowerShell, avoid here-strings with `python - << 'PY'`.)
