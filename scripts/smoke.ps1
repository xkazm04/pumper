<#
.SYNOPSIS
    Live-verification smoke loop for the pumper binary: boot, drive one real
    job end-to-end, curl the shipped read-only surfaces, teardown.

.DESCRIPTION
    Every other check in this repo (`just test`, `just lint`) verifies code
    paths in isolation. Nothing proves the shipped binary actually boots and
    the endpoints it exposes answer over real HTTP, against a real SQLite
    store, on a real port. This script is that proof:

      1. Locates (or builds once) the `pumper` binary.
      2. Creates an isolated scratch state dir under the OS temp folder, with
         its own config.toml pointing at a scratch DB / artifacts dir /
         search index and a non-default port (18099), so it never touches
         `./data` or collides with a `just run` already on 8088.
      3. Boots the server against that config and polls `GET /health` until
         it answers.
      4. Drives one real job end-to-end through the HTTP API (the
         `hackernews` example app — the simplest registered app with no
         required credentials; see NOTES). Network-dependent outcomes are
         reported SKIP rather than FAIL when this box can't reach the
         network, since the point of this script is proving the *server*
         works, not proving the internet is up.
      5. Curls a fixed checklist of shipped endpoints and asserts 200 + a
         sane JSON shape.
      6. Tears down: kills the server process and removes the scratch dir,
         in a `finally` block so a failed check still cleans up.
      7. Exits non-zero if any check FAILed (SKIPs don't fail the run).

.NOTES
    Why `hackernews`: it is the only app in `crates/apps/` whose `run()`
    needs no API key / env var / browser profile / paid engine — just plain
    HTTP against a public page (see `crates/apps/hackernews/src/lib.rs`,
    `requires() == []`). It is excluded from `catalog/data-sources.toml` as
    an "example/template app, not a production pipeline" (see the
    `CATALOG_EXEMPT` list in `crates/server/src/routes/mod.rs`), which is
    exactly why it's the right pick here: a smoke loop should not depend on
    any production pipeline's fixture data or scheduling. Every other app
    either needs network to a specific paid/keyed source, a browser profile,
    or the `claude` CLI — so this is the "cheapest local-ish" candidate the
    brief asks for, not a network-free one (none exists in this repo).

.PARAMETER Port
    Port the scratch server listens on. Default 18099 (clear of the default
    8088 so this can run alongside `just run`).

.PARAMETER KeepScratch
    Don't delete the scratch dir on exit (for debugging a failure).

.PARAMETER SkipBuild
    Fail instead of building if the binary isn't already present at the
    resolved path, rather than invoking `cargo build`. Useful in CI where the
    build is a separate, cached step.
#>
[CmdletBinding()]
param(
    [int]$Port = 18099,
    [switch]$KeepScratch,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$TargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $RepoRoot 'target' }
$BinExt = if ($IsWindows -or $env:OS -eq 'Windows_NT') { '.exe' } else { '' }
$BinPath = Join-Path $TargetDir "debug/pumper$BinExt"

$Scratch = Join-Path ([System.IO.Path]::GetTempPath()) "pumper-smoke-$([guid]::NewGuid().ToString('N').Substring(0,10))"
$DbPath = (Join-Path $Scratch 'pumper.db') -replace '\\', '/'
$ArtifactsDir = (Join-Path $Scratch 'artifacts') -replace '\\', '/'
$SearchDir = (Join-Path $Scratch 'search-index') -replace '\\', '/'
$ConfigPath = Join-Path $Scratch 'config.toml'
$ServerOutLog = Join-Path $Scratch 'server.out.log'
$ServerErrLog = Join-Path $Scratch 'server.err.log'

# ---------------------------------------------------------------------------
# Checklist bookkeeping
# ---------------------------------------------------------------------------

$script:Results = New-Object System.Collections.Generic.List[object]

function Add-Result {
    param([string]$Name, [ValidateSet('PASS', 'FAIL', 'SKIP')][string]$Status, [string]$Detail = '')
    $script:Results.Add([pscustomobject]@{ Name = $Name; Status = $Status; Detail = $Detail })
    $color = switch ($Status) { 'PASS' { 'Green' } 'FAIL' { 'Red' } 'SKIP' { 'Yellow' } }
    Write-Host ("[{0}] {1}{2}" -f $Status, $Name, $(if ($Detail) { " — $Detail" } else { '' })) -ForegroundColor $color
}

$script:ServerProcess = $null

function Stop-ScratchServer {
    if ($script:ServerProcess -and -not $script:ServerProcess.HasExited) {
        try {
            Stop-Process -Id $script:ServerProcess.Id -Force -ErrorAction Stop
            Write-Host "Stopped pumper server (pid $($script:ServerProcess.Id))."
        } catch {
            Write-Warning "Failed to stop pid $($script:ServerProcess.Id): $_"
        }
    }
    # Belt-and-braces: in case Start-Process spawned a child pid different
    # from the one we tracked, sweep anything still bound to our scratch
    # config — matched by command line, not by name, so a developer's own
    # `just run` (a different config) is never touched.
    try {
        Get-CimInstance Win32_Process -Filter "Name = 'pumper.exe'" -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -and $_.CommandLine -like "*$Scratch*" } |
            ForEach-Object {
                try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch {}
            }
    } catch {
        # Win32_Process/CIM unavailable (non-Windows, locked-down box) — the
        # tracked-pid stop above is the primary mechanism; this is only a
        # safety net.
    }
}

function Remove-Scratch {
    if ($KeepScratch) {
        Write-Host "KeepScratch set — leaving $Scratch for inspection."
        return
    }
    if (Test-Path $Scratch) {
        Remove-Item -Recurse -Force $Scratch -ErrorAction SilentlyContinue
    }
}

# ---------------------------------------------------------------------------
# 1. Locate / build the binary
# ---------------------------------------------------------------------------

Write-Host "=== pumper smoke loop ===" -ForegroundColor Cyan
Write-Host "repo root : $RepoRoot"
Write-Host "target dir: $TargetDir"
Write-Host "scratch   : $Scratch"
Write-Host "port      : $Port"
Write-Host ""

if (-not (Test-Path $BinPath)) {
    if ($SkipBuild) {
        Add-Result -Name 'binary present' -Status 'FAIL' -Detail "not found at $BinPath and -SkipBuild set"
        exit 1
    }
    Write-Host "Binary not found at $BinPath — building once (cargo build -p pumper-server --bin pumper)..."
    Push-Location $RepoRoot
    try {
        & cargo build -p pumper-server --bin pumper
        if ($LASTEXITCODE -ne 0) {
            Add-Result -Name 'cargo build' -Status 'FAIL' -Detail "exit code $LASTEXITCODE"
            exit 1
        }
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $BinPath)) {
    Add-Result -Name 'binary present' -Status 'FAIL' -Detail "still not found at $BinPath after build"
    exit 1
}
Add-Result -Name 'binary present' -Status 'PASS' -Detail $BinPath

# ---------------------------------------------------------------------------
# 2. Scratch state dir + config.toml
# ---------------------------------------------------------------------------

New-Item -ItemType Directory -Force -Path $Scratch | Out-Null

@"
[server]
port = $Port

[storage]
database_path = "$DbPath"
artifacts_dir = "$ArtifactsDir"

[search]
dir = "$SearchDir"
"@ | Set-Content -Path $ConfigPath -Encoding utf8

Add-Result -Name 'scratch config written' -Status 'PASS' -Detail $ConfigPath

# Everything from here must clean up on the way out, including on a thrown
# error — hence the single top-level try/finally wrapping boot through
# teardown.
$exitCode = 0
try {
    # -----------------------------------------------------------------------
    # 3. Boot + wait for readiness
    # -----------------------------------------------------------------------

    $env:PUMPER_CONFIG = $ConfigPath
    $env:RUST_LOG = 'info'

    $script:ServerProcess = Start-Process -FilePath $BinPath `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $ServerOutLog `
        -RedirectStandardError $ServerErrLog `
        -PassThru -WindowStyle Hidden

    $baseUrl = "http://127.0.0.1:$Port"
    $ready = $false
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        if ($script:ServerProcess.HasExited) {
            break
        }
        try {
            $resp = Invoke-WebRequest -Uri "$baseUrl/health" -UseBasicParsing -TimeoutSec 2
            if ($resp.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Milliseconds 300
        }
    }

    if (-not $ready) {
        $stderrTail = if (Test-Path $ServerErrLog) { Get-Content $ServerErrLog -Tail 40 -ErrorAction SilentlyContinue } else { @() }
        Add-Result -Name 'server boots and answers /health' -Status 'FAIL' `
            -Detail "did not become ready within 30s. stderr tail: $($stderrTail -join ' | ')"
        $exitCode = 1
        # No point running the rest of the checklist against a server that
        # never came up — but still fall through to `finally` for teardown.
    } else {
        Add-Result -Name 'server boots and answers /health' -Status 'PASS'

        # ---------------------------------------------------------------
        # 4. Drive one real job end-to-end (hackernews, see NOTES above)
        # ---------------------------------------------------------------
        $jobId = $null
        try {
            $enqueueResp = Invoke-RestMethod -Method Post -Uri "$baseUrl/apps/hackernews/jobs" `
                -Body (@{ params = @{ pages = 1 } } | ConvertTo-Json) -ContentType 'application/json' `
                -TimeoutSec 10
            $jobId = $enqueueResp.id
            if (-not $jobId) {
                Add-Result -Name 'enqueue hackernews job' -Status 'FAIL' -Detail 'response had no id'
            } else {
                Add-Result -Name 'enqueue hackernews job' -Status 'PASS' -Detail "job $jobId"
            }
        } catch {
            Add-Result -Name 'enqueue hackernews job' -Status 'FAIL' -Detail $_.Exception.Message
        }

        if ($jobId) {
            $jobDeadline = (Get-Date).AddSeconds(45)
            $finalStatus = $null
            $finalError = $null
            while ((Get-Date) -lt $jobDeadline) {
                $job = Invoke-RestMethod -Uri "$baseUrl/jobs/$jobId" -TimeoutSec 5
                if ($job.status -in @('succeeded', 'failed', 'cancelled')) {
                    $finalStatus = $job.status
                    $finalError = $job.error
                    break
                }
                Start-Sleep -Milliseconds 500
            }

            switch ($finalStatus) {
                'succeeded' {
                    Add-Result -Name 'job runs to completion (hackernews)' -Status 'PASS' -Detail "job $jobId succeeded"
                }
                'failed' {
                    $networkish = $finalError -and ($finalError -match 'dns|connect|timed? ?out|network|resolve|unreachable')
                    if ($networkish) {
                        Add-Result -Name 'job runs to completion (hackernews)' -Status 'SKIP' `
                            -Detail "no network to news.ycombinator.com from this box: $finalError"
                    } else {
                        Add-Result -Name 'job runs to completion (hackernews)' -Status 'FAIL' -Detail "job failed: $finalError"
                    }
                }
                default {
                    Add-Result -Name 'job runs to completion (hackernews)' -Status 'FAIL' -Detail "did not reach a terminal state within 45s (last: $finalStatus)"
                }
            }

            # ---------------------------------------------------------------
            # 5. Checklist of shipped read endpoints
            # ---------------------------------------------------------------

            function Test-JsonEndpoint {
                param(
                    [string]$Name,
                    [string]$Path,
                    [scriptblock]$Assert
                )
                try {
                    $resp = Invoke-WebRequest -Uri "$baseUrl$Path" -UseBasicParsing -TimeoutSec 15
                    if ($resp.StatusCode -ne 200) {
                        Add-Result -Name $Name -Status 'FAIL' -Detail "HTTP $($resp.StatusCode)"
                        return
                    }
                    $json = $resp.Content | ConvertFrom-Json
                    $ok = & $Assert $json
                    if ($ok) {
                        Add-Result -Name $Name -Status 'PASS'
                    } else {
                        Add-Result -Name $Name -Status 'FAIL' -Detail 'response JSON failed shape assertion'
                    }
                } catch {
                    Add-Result -Name $Name -Status 'FAIL' -Detail $_.Exception.Message
                }
            }

            Test-JsonEndpoint -Name 'GET /health' -Path '/health' -Assert { param($j) $j.status -eq 'ok' }

            Test-JsonEndpoint -Name 'GET /datasets/doctor' -Path '/datasets/doctor' -Assert {
                param($j) $null -ne $j.findings -and $null -ne $j.coverage -and $null -ne $j.tables
            }

            Test-JsonEndpoint -Name 'GET /retention/preview' -Path '/retention/preview' -Assert {
                param($j) $null -ne $j.PSObject.Properties
            }

            Test-JsonEndpoint -Name 'GET /enforcement/preview' -Path '/enforcement/preview' -Assert {
                param($j) $null -ne $j.ready -is [bool] -or $j.PSObject.Properties.Name -contains 'ready'
            }

            Test-JsonEndpoint -Name 'GET /openapi.json' -Path '/openapi.json' -Assert {
                param($j) $j.openapi -like '3.*' -and $j.info.title -eq 'pumper HTTP API'
            }

            Test-JsonEndpoint -Name 'GET /jobs/{id}/receipt' -Path "/jobs/$jobId/receipt" -Assert {
                param($j)
                $j.job.id -eq $jobId -and
                ($j.PSObject.Properties.Name -contains 'cost') -and
                ($j.PSObject.Properties.Name -contains 'unknown')
            }

            Test-JsonEndpoint -Name 'GET /search/status' -Path '/search/status' -Assert {
                param($j)
                ($j.PSObject.Properties.Name -contains 'doc_count') -and
                ($j.PSObject.Properties.Name -contains 'disk_bytes') -and
                ($j.PSObject.Properties.Name -contains 'segment_count')
            }

            # Raw check: the no-cursor response is a bare JSON array, and
            # PowerShell's pipeline unrolls an empty `[]` to $null before any
            # shape assertion could see it — so assert on the raw body.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/provisioner/proposals" -UseBasicParsing -TimeoutSec 15
                if ($resp.StatusCode -eq 200 -and $resp.Content.Trim().StartsWith('[')) {
                    Add-Result -Name 'GET /provisioner/proposals' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /provisioner/proposals' -Status 'FAIL' `
                        -Detail "HTTP $($resp.StatusCode), body starts '$($resp.Content.Substring(0, [Math]::Min(20, $resp.Content.Length)))'"
                }
            } catch {
                Add-Result -Name 'GET /provisioner/proposals' -Status 'FAIL' -Detail $_.Exception.Message
            }

            # Raw check: /metrics is Prometheus TEXT, not JSON, and the webhook
            # gauges must be present even with an empty delivery log (a gauge
            # that only appears once it is non-zero is a gauge you can't alert
            # on). Assert on the raw body, series by series.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/metrics" -UseBasicParsing -TimeoutSec 15
                $body = $resp.Content
                $wanted = @(
                    'pumper_webhook_deliveries{status="pending"}',
                    'pumper_webhook_deliveries{status="delivered"}',
                    'pumper_webhook_deliveries{status="failed"}',
                    'pumper_webhook_deliveries{status="dead"}',
                    'pumper_webhook_oldest_undelivered_seconds',
                    'pumper_webhook_delivery_attempts_total',
                    'pumper_webhook_deliveries_succeeded_total'
                )
                $missing = @($wanted | Where-Object { -not $body.Contains($_) })
                if ($resp.StatusCode -eq 200 -and $missing.Count -eq 0) {
                    Add-Result -Name 'GET /metrics carries the webhook delivery series' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /metrics carries the webhook delivery series' -Status 'FAIL' `
                        -Detail "HTTP $($resp.StatusCode), missing: $($missing -join ', ')"
                }
            } catch {
                Add-Result -Name 'GET /metrics carries the webhook delivery series' -Status 'FAIL' -Detail $_.Exception.Message
            }

            # A bogus ?status= must be a 400 naming the allowed values, NOT an
            # empty 200 — "no such deliveries" and "no such state" are opposite
            # answers on the endpoint that exists to surface undelivered hooks.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/webhooks/deliveries?status=dead-letter" -UseBasicParsing -TimeoutSec 15
                Add-Result -Name 'GET /webhooks/deliveries?status=<bogus> -> 400' -Status 'FAIL' `
                    -Detail "expected 400, got HTTP $($resp.StatusCode) with body '$($resp.Content)'"
            } catch {
                $code = $_.Exception.Response.StatusCode.value__
                if ($code -eq 400) {
                    Add-Result -Name 'GET /webhooks/deliveries?status=<bogus> -> 400' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /webhooks/deliveries?status=<bogus> -> 400' -Status 'FAIL' `
                        -Detail "expected 400, got: $($_.Exception.Message)"
                }
            }

            # ...and every real state is still accepted. Raw-body check: an
            # empty `deliveries` array unrolls to $null through the pipeline,
            # so a shape assertion on the parsed object can't tell 200-empty
            # from a failure.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/webhooks/deliveries?status=dead" -UseBasicParsing -TimeoutSec 15
                if ($resp.StatusCode -eq 200 -and $resp.Content.Contains('"deliveries"')) {
                    Add-Result -Name 'GET /webhooks/deliveries?status=dead (the DLQ view)' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /webhooks/deliveries?status=dead (the DLQ view)' -Status 'FAIL' `
                        -Detail "HTTP $($resp.StatusCode), body '$($resp.Content.Substring(0, [Math]::Min(80, $resp.Content.Length)))'"
                }
            } catch {
                Add-Result -Name 'GET /webhooks/deliveries?status=dead (the DLQ view)' -Status 'FAIL' -Detail $_.Exception.Message
            }

            # The scratch config ships [datahub] disabled, so the governance
            # preview must answer 409 — a 200 here would mean the bridge is on
            # against a GMS that does not exist.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/datahub/governance/preview" -UseBasicParsing -TimeoutSec 15
                Add-Result -Name 'GET /datahub/governance/preview (disabled -> 409)' -Status 'FAIL' `
                    -Detail "expected 409, got HTTP $($resp.StatusCode)"
            } catch {
                $code = $_.Exception.Response.StatusCode.value__
                if ($code -eq 409) {
                    Add-Result -Name 'GET /datahub/governance/preview (disabled -> 409)' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /datahub/governance/preview (disabled -> 409)' -Status 'FAIL' `
                        -Detail "expected 409, got: $($_.Exception.Message)"
                }
            }
        } else {
            Add-Result -Name 'job runs to completion (hackernews)' -Status 'SKIP' -Detail 'no job id to poll (enqueue failed)'
            Add-Result -Name 'GET /jobs/{id}/receipt' -Status 'SKIP' -Detail 'no job id to fetch a receipt for'
            # The rest of the checklist doesn't depend on the job, so still run it.
            function Test-JsonEndpointNoJob {
                param([string]$Name, [string]$Path, [scriptblock]$Assert)
                try {
                    $resp = Invoke-WebRequest -Uri "$baseUrl$Path" -UseBasicParsing -TimeoutSec 15
                    if ($resp.StatusCode -ne 200) { Add-Result -Name $Name -Status 'FAIL' -Detail "HTTP $($resp.StatusCode)"; return }
                    $json = $resp.Content | ConvertFrom-Json
                    if (& $Assert $json) { Add-Result -Name $Name -Status 'PASS' } else { Add-Result -Name $Name -Status 'FAIL' -Detail 'shape assertion failed' }
                } catch {
                    Add-Result -Name $Name -Status 'FAIL' -Detail $_.Exception.Message
                }
            }
            Test-JsonEndpointNoJob -Name 'GET /health' -Path '/health' -Assert { param($j) $j.status -eq 'ok' }
            Test-JsonEndpointNoJob -Name 'GET /datasets/doctor' -Path '/datasets/doctor' -Assert { param($j) $null -ne $j.findings }
            Test-JsonEndpointNoJob -Name 'GET /retention/preview' -Path '/retention/preview' -Assert { param($j) $true }
            Test-JsonEndpointNoJob -Name 'GET /enforcement/preview' -Path '/enforcement/preview' -Assert { param($j) $true }
            Test-JsonEndpointNoJob -Name 'GET /openapi.json' -Path '/openapi.json' -Assert { param($j) $j.openapi -like '3.*' }
        }
    }
} finally {
    # -----------------------------------------------------------------------
    # 6. Teardown — always runs, including on a thrown error above.
    # -----------------------------------------------------------------------
    Stop-ScratchServer
    Remove-Scratch
}

# ---------------------------------------------------------------------------
# 7. Summary + exit code
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "=== summary ===" -ForegroundColor Cyan
$script:Results | ForEach-Object {
    $color = switch ($_.Status) { 'PASS' { 'Green' } 'FAIL' { 'Red' } 'SKIP' { 'Yellow' } }
    Write-Host ("  [{0,-4}] {1}" -f $_.Status, $_.Name) -ForegroundColor $color
}
$failCount = ($script:Results | Where-Object { $_.Status -eq 'FAIL' }).Count
$passCount = ($script:Results | Where-Object { $_.Status -eq 'PASS' }).Count
$skipCount = ($script:Results | Where-Object { $_.Status -eq 'SKIP' }).Count
Write-Host ""
Write-Host "$passCount passed, $failCount failed, $skipCount skipped."

if ($failCount -gt 0 -or $exitCode -ne 0) {
    exit 1
}
exit 0
