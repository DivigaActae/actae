# Runtime smoke test for a Windows release binary. Mirrors release-smoke.sh
# including restart persistence with the packaged binary.
param(
    [Parameter(Mandatory = $true)]
    [string]$Bin
)

$ErrorActionPreference = "Stop"
$DataDir = Join-Path $env:TEMP ("actae-smoke-" + [guid]::NewGuid().ToString("N"))
$HttpPort = 18102
$PgPort = 18106
$Key = "sk-smoke-00000000000000000000000000000000"
$Log = Join-Path $DataDir "smoke.log"
New-Item -ItemType Directory -Path $DataDir -Force | Out-Null

function Stop-Actae {
    Get-Process | Where-Object { $_.Path -eq $Bin } | Stop-Process -Force -ErrorAction SilentlyContinue
}

function Start-And-Wait {
    $env:ACTAE_PORT = "$HttpPort"
    $env:ACTAE_EMBEDDED_PG_PORT = "$PgPort"
    $env:ACTAE_DATA_DIR = $DataDir
    $env:ACTAE_INITIAL_API_KEY = $Key
    $script:proc = Start-Process -FilePath $Bin -RedirectStandardOutput $Log -RedirectStandardError $Log -PassThru
    for ($i = 0; $i -lt 120; $i++) {
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:$HttpPort/healthz" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { return }
        } catch {}
        Start-Sleep -Seconds 1
    }
    throw "server did not become healthy (see $Log)"
}

try {
    Write-Host "== smoke: $Bin =="

    Write-Host "  first start"
    Start-And-Wait

    $body = '{"channel_id":"smoke","event_type":"smoke","payload":{"ok":true},"metadata":{"actor":"smoke"}}'
    Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$HttpPort/api/v1/events/record" `
        -Headers @{"X-API-Key"=$Key} -ContentType "application/json" -Body $body | Out-Null

    $replay = Invoke-RestMethod -Uri "http://127.0.0.1:$HttpPort/api/v1/events/replay/smoke?cursor=0" `
        -Headers @{"X-API-Key"=$Key}
    if ($replay.events.Count -lt 1) { throw "replay did not contain the event" }

    $dashboard = (Invoke-WebRequest -Uri "http://127.0.0.1:$HttpPort/" -UseBasicParsing).Content
    if (-not $dashboard -match "<html") { throw "dashboard not served" }

    Write-Host "  graceful shutdown"
    Stop-Process -Id $proc.Id -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 5

    Write-Host "  restart (same data dir) — verifying persistence"
    Start-And-Wait
    $replay2 = Invoke-RestMethod -Uri "http://127.0.0.1:$HttpPort/api/v1/events/replay/smoke?cursor=0" `
        -Headers @{"X-API-Key"=$Key}
    if ($replay2.events.Count -lt 1) { throw "event did not survive a restart" }
    Stop-Process -Id $proc.Id -ErrorAction SilentlyContinue

    Write-Host "  OK: $Bin"
}
finally {
    Stop-Actae
    Remove-Item -Recurse -Force $DataDir -ErrorAction SilentlyContinue
}
