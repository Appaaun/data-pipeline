# Entry point for schedulers (Windows Task Scheduler, cron via WSL, CI).
# Exit code: 0 = everything succeeded, 1 = a source or gold model failed, 2 = config error.
param(
    [string[]]$Source = @(),          # e.g. -Source shop_orders,stock_movements
    [switch]$SkipGold,
    [switch]$FullRefresh
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$args = @("run")
foreach ($s in $Source) { $args += @("-s", $s) }
if ($SkipGold)    { $args += "--skip-gold" }
if ($FullRefresh) { $args += "--full-refresh" }

& "$root\.venv\Scripts\pipeline.exe" @args
exit $LASTEXITCODE
