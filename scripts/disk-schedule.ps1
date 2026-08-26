<#
.SYNOPSIS
  Register (or remove) the weekly build-cache prune as a Windows scheduled task.

.DESCRIPTION
  `just disk-check` already self-heals, but it only runs when someone runs
  `just ci`. This covers the gap it cannot: sessions that build fifty times and
  never reach a gate, which is exactly how `target/` reached 280.8 GB in a single
  month (measured 2026-08-26).

  Per-user task, no elevation required. The task runs
  `node scripts/ci/disk-check.mjs --prune` from the repo root, which deletes only
  superseded artifact generations and week-old incremental sessions — never a
  live artifact. See scripts/ci/disk-check.mjs.

  This is the one thing in the repo that writes state OUTSIDE the checkout, so it
  is opt-in and never runs as part of a build.

.PARAMETER Remove
  Unregister the task instead of creating it.

.PARAMETER At
  Time of day to run. Default 03:00.
#>
[CmdletBinding()]
param(
  [switch]$Remove,
  [string]$At = '3:00am'
)

$ErrorActionPreference = 'Stop'
$TaskName = 'pumper-disk-prune'

if ($Remove) {
  $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  if ($null -eq $existing) {
    Write-Output "not registered: $TaskName (nothing to remove)"
    exit 0
  }
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
  Write-Output "removed: $TaskName"
  exit 0
}

# Resolve from THIS script's location rather than the caller's CWD: a scheduled
# task with the wrong working directory prunes nothing and reports success.
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Script = Join-Path $RepoRoot 'scripts/ci/disk-check.mjs'
if (-not (Test-Path $Script)) {
  Write-Error "cannot find $Script — refusing to register a task that would do nothing."
  exit 1
}

$node = Get-Command node -ErrorAction SilentlyContinue
if ($null -eq $node) {
  Write-Error 'node is not on PATH — the scheduled task would fail silently every week.'
  exit 1
}

$action = New-ScheduledTaskAction -Execute $node.Source `
  -Argument 'scripts/ci/disk-check.mjs --prune' -WorkingDirectory $RepoRoot
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $At
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
  -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
  -Settings $settings -Description 'Prune superseded pumper build artifacts' -Force | Out-Null

Write-Output "registered: $TaskName (weekly, Sunday $At)"
Write-Output "  runs: $($node.Source) scripts/ci/disk-check.mjs --prune"
Write-Output "  in:   $RepoRoot"
Write-Output "remove with: just disk-unschedule"
