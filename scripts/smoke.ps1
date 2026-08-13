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
    Run whatever binary is already present at the resolved path (failing if
    there is none) instead of building. Useful in CI where the build is a
    separate, cached step. Without it the script always runs `cargo build`
    first — an up-to-date incremental build is a no-op costing seconds, and it
    guarantees the smoke run exercises THIS tree's code rather than silently
    reusing a stale binary.
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

if ($SkipBuild) {
    if (-not (Test-Path $BinPath)) {
        Add-Result -Name 'binary present' -Status 'FAIL' -Detail "not found at $BinPath and -SkipBuild set"
        exit 1
    }
} else {
    # Always build: an up-to-date incremental build is a no-op costing seconds,
    # and skipping it silently smoke-tests a STALE binary when one is lying
    # around (found the hard way in round 8 — the checks passed against code
    # that wasn't the tree's).
    Write-Host "Building pumper (cargo build -p pumper-server --bin pumper; incremental no-op when fresh)..."
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

            # The remote fetch fabric's egress split. BOTH series must be present
            # with the fabric OFF (this scratch config has no [remote] nodes):
            # an absent series and a zero series are different answers to "did any
            # egress leave through a peer", and a fabric that is silently falling
            # back to the coordinator's own IP is exactly what this exists to show.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/metrics" -UseBasicParsing -TimeoutSec 20
                $body = $resp.Content
                $wanted = @(
                    'pumper_remote_egress_fetches{served_by="peer"} 0',
                    'pumper_remote_egress_fetches{served_by="local_fallback"} 0'
                )
                $missing = @($wanted | Where-Object { -not $body.Contains($_) })
                if ($resp.StatusCode -eq 200 -and $missing.Count -eq 0) {
                    Add-Result -Name 'GET /metrics carries the remote egress series' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /metrics carries the remote egress series' -Status 'FAIL' `
                        -Detail "HTTP $($resp.StatusCode), missing: $($missing -join ', ')"
                }
            } catch {
                Add-Result -Name 'GET /metrics carries the remote egress series' -Status 'FAIL' -Detail $_.Exception.Message
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

            # Round 10: the transact door refuses garbage AT ENQUEUE. Each bad
            # payload must be a 422 whose envelope carries code "unprocessable"
            # — a 202 here means the schema tightening regressed and a flow
            # would burn a browser run to rediscover the refusal. (The graceful
            # -shutdown bound cannot be driven from this script: Windows has no
            # way to deliver Ctrl-C to a detached hidden process, so that path
            # stays proven at the e2e layer only.)
            function Test-TransactDoor {
                param([string]$Name, [hashtable]$JobParams)
                try {
                    $resp = Invoke-WebRequest -Method Post -Uri "$baseUrl/apps/transact/jobs" `
                        -Body (@{ params = $JobParams } | ConvertTo-Json -Depth 8) `
                        -ContentType 'application/json' -UseBasicParsing -TimeoutSec 10
                    Add-Result -Name $Name -Status 'FAIL' `
                        -Detail "expected 422, got HTTP $($resp.StatusCode) — the door let it through"
                } catch {
                    $code = $_.Exception.Response.StatusCode.value__
                    $body = ''
                    try { $body = $_.ErrorDetails.Message } catch {}
                    if ($code -eq 422 -and $body -like '*"unprocessable"*') {
                        Add-Result -Name $Name -Status 'PASS'
                    } else {
                        Add-Result -Name $Name -Status 'FAIL' `
                            -Detail "expected 422 + code 'unprocessable', got HTTP $code body '$body'"
                    }
                }
            }
            $transactBase = @{
                url             = 'https://example.com/form'
                idempotency_key = 'smoke-door-1'
                submit_action   = @{ action = 'click'; selector = '#go' }
            }
            $withSubmit = $transactBase.Clone(); $withSubmit.submit = $true
            Test-TransactDoor -Name 'POST /apps/transact/jobs submit:true -> 422' -JobParams $withSubmit
            $withBlankKey = $transactBase.Clone(); $withBlankKey.idempotency_key = '   '
            Test-TransactDoor -Name 'POST /apps/transact/jobs blank idempotency_key -> 422' -JobParams $withBlankKey
            $withTypo = $transactBase.Clone(); $withTypo.step = @(@{ action = 'click'; selector = '#next' })
            Test-TransactDoor -Name "POST /apps/transact/jobs typo'd 'step' key -> 422" -JobParams $withTypo
            # An unsafe profile name must die at the DOOR, not become a job that
            # re-refuses on every attempt. The schema's pattern is generated from
            # the validator's own alphabet, so the door and the engine cannot
            # disagree about what a legal profile is.
            $withBadProfile = $transactBase.Clone(); $withBadProfile.profile = '../escape'
            Test-TransactDoor -Name 'POST /apps/transact/jobs unsafe profile name -> 422' -JobParams $withBadProfile

            # ...and the manifest DECLARES the closed door, so a consumer
            # reading the tool definition sees the same contract the door
            # enforces (a regression here ships a lying schema even if the
            # server-side validator still rejects).
            Test-JsonEndpoint -Name 'GET /apps?format=tools declares the closed transact door' `
                -Path '/apps?format=tools' -Assert {
                param($j)
                $t = @($j.tools | Where-Object { $_.name -eq 'transact' })[0]
                if (-not $t) { return $false }
                ($t.inputSchema.properties.submit.const -eq $false) -and
                ($t.inputSchema.additionalProperties -eq $false)
            }

            # Round 11: door parity + watch honesty, live. Every door that
            # creates future work answers with the job door's 422, and a watch
            # that structurally cannot fire is refused with the namespace the
            # records actually land under.
            function Test-DoorRefusal {
                param([string]$Name, [string]$Path, [hashtable]$Body, [int]$Expect, [string]$BodyLike)
                try {
                    $resp = Invoke-WebRequest -Method Post -Uri "$baseUrl$Path" `
                        -Body ($Body | ConvertTo-Json -Depth 8) `
                        -ContentType 'application/json' -UseBasicParsing -TimeoutSec 10
                    Add-Result -Name $Name -Status 'FAIL' `
                        -Detail "expected $Expect, got HTTP $($resp.StatusCode) — the door let it through"
                } catch {
                    $code = $_.Exception.Response.StatusCode.value__
                    $errBody = ''
                    try { $errBody = $_.ErrorDetails.Message } catch {}
                    if ($code -eq $Expect -and $errBody -like $BodyLike) {
                        Add-Result -Name $Name -Status 'PASS'
                    } else {
                        Add-Result -Name $Name -Status 'FAIL' `
                            -Detail "expected $Expect + body like '$BodyLike', got HTTP $code body '$errBody'"
                    }
                }
            }
            # The schedules door refuses what the job door refuses (bogus key
            # under transact's additionalProperties:false schema) instead of
            # storing a standing order whose every firing would fail.
            Test-DoorRefusal -Name 'POST /schedules invalid params -> 422' -Path '/schedules' `
                -Body @{ app = 'transact'; cron = '0 0 0 * * *'; params = @{ bogus_key = 1 } } `
                -Expect 422 -BodyLike '*"unprocessable"*'
            # (The ca-grants/unified trap-400 is store-derived — it needs a
            # 'unified' dataset to exist under 'grants' — so a fresh scratch DB
            # legitimately answers 201 and the trap stays e2e-proven only. The
            # state-independent honesty surface is `last_delivery` below.)
            # The virtual namespace every grant revision lands under is watchable.
            try {
                $resp = Invoke-WebRequest -Method Post -Uri "$baseUrl/watches" `
                    -Body (@{ app = 'grants'; sink = 'file' } | ConvertTo-Json) `
                    -ContentType 'application/json' -UseBasicParsing -TimeoutSec 10
                if ($resp.StatusCode -eq 201) {
                    Add-Result -Name 'POST /watches app=grants (virtual namespace) -> 201' -Status 'PASS'
                } else {
                    Add-Result -Name 'POST /watches app=grants (virtual namespace) -> 201' -Status 'FAIL' `
                        -Detail "expected 201, got HTTP $($resp.StatusCode)"
                }
            } catch {
                Add-Result -Name 'POST /watches app=grants (virtual namespace) -> 201' -Status 'FAIL' `
                    -Detail $_.Exception.Message
            }
            # A bogus ?app= filter is a 400 with the known values, not an empty 200.
            try {
                $resp = Invoke-WebRequest -Uri "$baseUrl/watches?app=definitely-not-an-app" `
                    -UseBasicParsing -TimeoutSec 10
                Add-Result -Name 'GET /watches?app=bogus -> 400' -Status 'FAIL' `
                    -Detail "expected 400, got HTTP $($resp.StatusCode)"
            } catch {
                $code = $_.Exception.Response.StatusCode.value__
                if ($code -eq 400) {
                    Add-Result -Name 'GET /watches?app=bogus -> 400' -Status 'PASS'
                } else {
                    Add-Result -Name 'GET /watches?app=bogus -> 400' -Status 'FAIL' `
                        -Detail "expected 400, got HTTP $code"
                }
            }
            # A never-fired watch says so: `last_delivery` is an explicit null,
            # not an omitted key — the surface that reveals an accepted-but-dead
            # watch (the trap the create-door cannot catch on an empty store).
            Test-JsonEndpoint -Name 'GET /watches carries explicit last_delivery' -Path '/watches' -Assert {
                param($j)
                $w = @($j.watches | Where-Object { $_.app -eq 'grants' })[0]
                if (-not $w) { return $false }
                $w.PSObject.Properties.Name -contains 'last_delivery'
            }

            # Round 12: mode exclusivity, the budget floor at both doors, and
            # the search answer's index-state block — all state-independent.
            # A params object carrying several extractor mode roots is refused
            # at the door (the app used to run the first match and return 200).
            Test-DoorRefusal -Name 'POST /apps/extractor/jobs conflicting modes -> 422' `
                -Path '/apps/extractor/jobs' `
                -Body @{ params = @{
                        rules  = @{ title = @{ type = 'css'; selector = 'h1' } }
                        urls   = @('https://example.com/')
                        replay = @{ rules = @{ title = @{ type = 'css'; selector = 'h1' } } }
                    } } `
                -Expect 422 -BodyLike '*"unprocessable"*'
            # budget_usd: 0 is a refusal, not a silent "no ceiling" — at the
            # jobs door and at the trigger door (where the dropped value would
            # be replayed into every hop: a standing unlimited-spend generator).
            Test-DoorRefusal -Name 'POST /apps/hackernews/jobs budget_usd:0 -> 422' `
                -Path '/apps/hackernews/jobs' `
                -Body @{ budget_usd = 0.0 } `
                -Expect 422 -BodyLike '*NO spend ceiling*'
            Test-DoorRefusal -Name 'POST /triggers budget_usd:0 -> 422' `
                -Path '/triggers' `
                -Body @{ source_kind = 'job'; source_app = 'hackernews'; target_app = 'hackernews'; budget_usd = 0.0 } `
                -Expect 422 -BodyLike '*NO spend ceiling*'
            # Every search answer names the index it came from: the additive
            # `index` block with an explicit degraded verdict, so `total: 0`
            # from a wiped/disabled index can never read as "no matches".
            # Shape-only on purpose — whether THIS scratch index is populated
            # by now is state; the honesty surface is the block itself.
            Test-JsonEndpoint -Name 'GET /search carries the index-state block' `
                -Path '/search?q=smoke' -Assert {
                param($j)
                $names = $j.index.PSObject.Properties.Name
                ($names -contains 'enabled') -and ($names -contains 'doc_count') -and
                ($names -contains 'degraded') -and ($names -contains 'reason')
            }

            # Round 13: schedules were the last work-creating door without the
            # budget floor. Same contract as the jobs/trigger doors: 0 is a
            # refusal, never a silent "no ceiling" stored on a row that replays
            # it into every firing forever.
            Test-DoorRefusal -Name 'POST /schedules budget_usd:0 -> 422' `
                -Path '/schedules' `
                -Body @{ app = 'hackernews'; cron = '0 0 3 * * *'; budget_usd = 0.0 } `
                -Expect 422 -BodyLike '*NO spend ceiling*'
            # ...and the late door (the only way a code-seeded/catalog row ever
            # gets a ceiling) answers 404 honestly for a row that isn't there.
            Test-DoorRefusal -Name 'POST /schedules/{id}/budget unknown id -> 404' `
                -Path '/schedules/definitely-not-a-schedule/budget' `
                -Body @{ budget_usd = 1.5 } `
                -Expect 404 -BodyLike '*not found*'
            # A real ceiling round-trips: created with one, listed with one.
            try {
                $resp = Invoke-WebRequest -Method Post -Uri "$baseUrl/schedules" `
                    -Body (@{ app = 'hackernews'; cron = '0 0 3 * * *'; budget_usd = 0.75 } | ConvertTo-Json) `
                    -ContentType 'application/json' -UseBasicParsing -TimeoutSec 10
                $row = $resp.Content | ConvertFrom-Json
                if ($resp.StatusCode -eq 201 -and $row.budget_usd -eq 0.75) {
                    Add-Result -Name 'POST /schedules stores + returns budget_usd' -Status 'PASS'
                } else {
                    Add-Result -Name 'POST /schedules stores + returns budget_usd' -Status 'FAIL' `
                        -Detail "HTTP $($resp.StatusCode), budget_usd '$($row.budget_usd)'"
                }
                # Clean up so the scratch schedule can't fire mid-teardown.
                Invoke-WebRequest -Method Delete -Uri "$baseUrl/schedules/$($row.id)" `
                    -UseBasicParsing -TimeoutSec 10 | Out-Null
            } catch {
                Add-Result -Name 'POST /schedules stores + returns budget_usd' -Status 'FAIL' `
                    -Detail $_.Exception.Message
            }

            # Round 14: the plugin host stopped lying. Every /plugins entry now
            # says whether it is actually runnable (`executable`) and what it
            # has cost (`telemetry` — present with calls:0 for a never-run
            # plugin, so "never invoked" is distinguishable from "unmetered").
            # Shape-only: whether THIS scratch server has any .wasm installed
            # is state; the honesty surface is the fields.
            Test-JsonEndpoint -Name 'GET /plugins entries carry executable + telemetry' `
                -Path '/plugins' -Assert {
                param($j)
                if ($null -eq $j.PSObject.Properties['plugins']) { return $false }
                foreach ($p in @($j.plugins)) {
                    $names = $p.PSObject.Properties.Name
                    if (($names -notcontains 'executable') -or ($names -notcontains 'telemetry')) {
                        return $false
                    }
                    if ($null -eq $p.telemetry.PSObject.Properties['calls']) { return $false }
                }
                $true
            }
            # A dry-run of a trigger gated by a plugin nobody installed used to
            # answer a clean `would_fire: true` — the exact mis-deployment the
            # operator is dry-running to discover. It now names the unusable
            # plugin while still reporting the fail-open verdict honestly.
            try {
                $resp = Invoke-WebRequest -Method Post -Uri "$baseUrl/triggers" `
                    -Body (@{
                        source_kind  = 'job'; source_app = 'hackernews'; target_app = 'hackernews'
                        # NB: the API field is `plugins` (docs/features/trigger-plugins.md);
                        # `plugin_hooks` is the storage column and the door ignores it.
                        plugins      = @{ predicate = @{ plugin = 'not-a-real-plugin' } }
                    } | ConvertTo-Json -Depth 5) `
                    -ContentType 'application/json' -UseBasicParsing -TimeoutSec 10
                $trig = $resp.Content | ConvertFrom-Json
                $test = Invoke-WebRequest -Method Post -Uri "$baseUrl/triggers/$($trig.id)/test" `
                    -UseBasicParsing -TimeoutSec 15
                $dry = $test.Content | ConvertFrom-Json
                $unusable = @($dry.hooks.unusable_plugins)
                if ($test.StatusCode -eq 200 -and $dry.would_fire -eq $true -and
                    $unusable -contains 'not-a-real-plugin') {
                    Add-Result -Name 'trigger dry-run names its unusable hook plugin' -Status 'PASS'
                } else {
                    Add-Result -Name 'trigger dry-run names its unusable hook plugin' -Status 'FAIL' `
                        -Detail "HTTP $($test.StatusCode), would_fire '$($dry.would_fire)', unusable [$($unusable -join ',')]"
                }
                Invoke-WebRequest -Method Delete -Uri "$baseUrl/triggers/$($trig.id)" `
                    -UseBasicParsing -TimeoutSec 10 | Out-Null
            } catch {
                Add-Result -Name 'trigger dry-run names its unusable hook plugin' -Status 'FAIL' `
                    -Detail $_.Exception.Message
            }

            # Round 19: a plugin job that cannot run must not report success.
            # THE ANTI-PATTERN: the run door checked only that `plugin` was a
            # STRING, so a typo (or, as here, a scratch deployment with no
            # modules installed at all) produced one `{"error": ...}` record per
            # URL, `ran: 0`, an empty dataset — and a SUCCEEDED job, green on
            # `GET /jobs`, with a `succeeded` SSE event and a fired webhook.
            # This is the smoke-level before/after: the door now refuses at
            # `run()` before any fetch, so the job FAILS and says which plugin.
            # `just plugins-install` is deliberately NOT a precondition — the
            # nothing-is-loaded case is exactly the one that used to lie.
            $pluginJobId = $null
            try {
                $pluginEnqueue = Invoke-RestMethod -Method Post -Uri "$baseUrl/apps/plugin/jobs" `
                    -Body (@{ params = @{
                        plugin = 'definitely-not-a-loaded-plugin'
                        urls   = @('https://example.com/')
                    } } | ConvertTo-Json -Depth 5) -ContentType 'application/json' -TimeoutSec 10
                $pluginJobId = $pluginEnqueue.id
            } catch {
                Add-Result -Name 'plugin job with an unloadable plugin fails (not succeeds)' -Status 'FAIL' `
                    -Detail "enqueue threw: $($_.Exception.Message)"
            }
            if ($pluginJobId) {
                $pDeadline = (Get-Date).AddSeconds(30)
                $pStatus = $null; $pError = $null
                while ((Get-Date) -lt $pDeadline) {
                    $pj = Invoke-RestMethod -Uri "$baseUrl/jobs/$pluginJobId" -TimeoutSec 5
                    if ($pj.status -in @('succeeded', 'failed', 'cancelled')) {
                        $pStatus = $pj.status; $pError = $pj.error; break
                    }
                    Start-Sleep -Milliseconds 400
                }
                # The failure must NAME the plugin and point at the surface that
                # lists the real ones — "unknown error" would be the same lie in
                # a different costume.
                if ($pStatus -eq 'failed' -and $pError -match 'definitely-not-a-loaded-plugin') {
                    Add-Result -Name 'plugin job with an unloadable plugin fails (not succeeds)' -Status 'PASS' `
                        -Detail "job $pluginJobId failed: $pError"
                } else {
                    Add-Result -Name 'plugin job with an unloadable plugin fails (not succeeds)' -Status 'FAIL' `
                        -Detail "status '$pStatus', error '$pError' (expected failed naming the plugin)"
                }
            }

            # Round 19: the plugin app's manifest DECLARES the bounds its code
            # enforces. `docs/features/extraction.md` claimed the concurrency
            # ceiling was "enforced twice — refused at the door and clamped in
            # code, so the two layers cannot disagree" and named this app, while
            # `parse_concurrency` clamped only the lower end. A consumer reads
            # the tool definition, so a lying schema is a lying contract even
            # when the server-side validator still rejects.
            Test-JsonEndpoint -Name 'GET /apps?format=tools declares the plugin app bounds' `
                -Path '/apps?format=tools' -Assert {
                param($j)
                $t = @($j.tools | Where-Object { $_.name -eq 'plugin' })[0]
                if (-not $t) { return $false }
                ($t.inputSchema.properties.concurrency.maximum -eq 64) -and
                ($t.inputSchema.properties.records_echo.maximum -eq 1000)
            }

            # Round 20: a `replay_of` job against an app that cannot be replayed
            # must be REFUSED BY NAME, before the app runs.
            # THE ANTI-PATTERN: `vcr.rs` promised "replay runs touch no engine",
            # but the cassette is written and read at ONE seam
            # (AppContext::fetch/research) and 17 app crates reach engines
            # outside it. `hackernews` is one of them, so this exact request used
            # to RUN THE APP LIVE — real network, real cost — and the worker then
            # stamped `vcr_replay_of` on the stored result, which is a provenance
            # claim that the output came from recorded bytes.
            # This is the smoke-level before/after. It is deliberately run
            # against the job the smoke ALREADY recorded nothing for: the refusal
            # must come from the app's declared fidelity
            # (`vcr::REPLAY_BYPASS_APPS`), not from a missing cassette file —
            # which is why the error has to name the app and the reason rather
            # than say "no cassette", the operator-blaming message that used to
            # be the only thing standing in the way.
            $replayJobId = $null
            try {
                $replayEnqueue = Invoke-RestMethod -Method Post -Uri "$baseUrl/apps/hackernews/jobs" `
                    -Body (@{ params = @{ replay_of = $jobId } } | ConvertTo-Json -Depth 5) `
                    -ContentType 'application/json' -TimeoutSec 10
                $replayJobId = $replayEnqueue.id
            } catch {
                Add-Result -Name 'replay_of an unreplayable app is refused by name' -Status 'FAIL' `
                    -Detail "enqueue threw: $($_.Exception.Message)"
            }
            if ($replayJobId) {
                $rDeadline = (Get-Date).AddSeconds(30)
                $rStatus = $null; $rError = $null; $rAttempts = $null
                while ((Get-Date) -lt $rDeadline) {
                    $rj = Invoke-RestMethod -Uri "$baseUrl/jobs/$replayJobId" -TimeoutSec 5
                    if ($rj.status -in @('succeeded', 'failed', 'cancelled')) {
                        $rStatus = $rj.status; $rError = $rj.error; $rAttempts = $rj.attempts; break
                    }
                    Start-Sleep -Milliseconds 400
                }
                # Terminal on the FIRST attempt: an app is what it is on every
                # attempt, so the backoff ladder cannot change the answer. A
                # refusal that burned four attempts would be the same defect in
                # a different costume.
                if ($rStatus -eq 'failed' -and $rError -match 'hackernews' -and
                    $rError -match 'cannot be replayed' -and $rAttempts -le 1) {
                    Add-Result -Name 'replay_of an unreplayable app is refused by name' -Status 'PASS' `
                        -Detail "job $replayJobId failed on attempt $rAttempts : $rError"
                } else {
                    Add-Result -Name 'replay_of an unreplayable app is refused by name' -Status 'FAIL' `
                        -Detail "status '$rStatus', attempts '$rAttempts', error '$rError' (expected failed on attempt 1 naming hackernews)"
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
