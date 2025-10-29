param()
$ErrorActionPreference = "Stop"

function Step($name, [scriptblock]$block) {
  Write-Host "== $name ==" -ForegroundColor Cyan
  & $block
  if ($LASTEXITCODE -ne 0) { throw "❌ Failed: $name" }
}

# Activate venv if not already
if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  throw "No venv found at .venv. Create one and install deps first."
}
. .\.venv\Scripts\Activate.ps1

Step "ruff check" { ruff check . }
Step "ruff format --check" { ruff format --check . }
Step "pre-commit (all files)" { pre-commit run --all-files }
Step "pytest" { pytest -q }
Step "detect-secrets hook" { pre-commit run detect-secrets --all-files }

Write-Host "✔ All R04 local checks passed" -ForegroundColor Green
