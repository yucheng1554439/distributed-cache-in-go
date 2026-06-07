# Repeatable load-test benchmarks for distributed-cache.
# Usage: .\scripts\benchmark.ps1 [-Duration 15s] [-Concurrency 64] [-SkipCluster]

param(
    [string]$Duration = "15s",
    [int]$Concurrency = 64,
    [switch]$SkipCluster
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$NodeBin = Join-Path $Root "node-test.exe"
$LoadBin = Join-Path $Root "loadtest-test.exe"
if (-not (Test-Path $NodeBin)) { go build -o $NodeBin ./cmd/node | Out-Null }
if (-not (Test-Path $LoadBin)) { go build -o $LoadBin ./cmd/loadtest | Out-Null }

$AddrMap = "node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381"
$ComposeFile = "deployments/docker/docker-compose.yml"
$ComposeRF1 = "deployments/docker/docker-compose.rf1.yml"
$ResultsDir = "benchmark-results"
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RunDir = Join-Path $ResultsDir $Timestamp
$Peers = "node-a=127.0.0.1:6379,node-b=127.0.0.1:6380,node-c=127.0.0.1:6381"

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

function Invoke-LoadTest {
    param(
        [string]$Name,
        [string]$Addr,
        [string[]]$ExtraArgs = @()
    )

    Write-Host "`n=== $Name ===" -ForegroundColor Cyan
    $jsonPath = Join-Path $RunDir "$Name.json"
    $args = @(
        "-addr", $Addr,
        "-duration", $Duration,
        "-concurrency", "$Concurrency",
        "-get-ratio", "0.8",
        "-timeout", "5s",
        "-json"
    ) + $ExtraArgs

    $output = & $LoadBin @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "loadtest failed for $Name`: $output"
    }
    $output | Out-File -FilePath $jsonPath -Encoding utf8
    Write-Host $output
    return ($output | ConvertFrom-Json)
}

function Wait-Port {
    param([string]$Endpoint, [int]$TimeoutSec = 30)
    $hostName, $portText = $Endpoint -split ":"
    if ($hostName -eq "") { $hostName = "127.0.0.1" }
    $port = [int]$portText
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = New-Object System.Net.Sockets.TcpClient
            $client.ConnectAsync($hostName, $port).Wait(500) | Out-Null
            if ($client.Connected) {
                $client.Close()
                return
            }
            $client.Close()
        } catch {}
        Start-Sleep -Milliseconds 250
    }
    throw "port $Endpoint did not become ready within ${TimeoutSec}s"
}

function Stop-Procs([System.Diagnostics.Process[]]$Procs) {
    foreach ($proc in $Procs) {
        if ($null -ne $proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

function Start-LocalCluster {
    param([int]$ReplicationFactor, [switch]$Raft)
    $procs = @()
    $argsCommon = @("-peers", $Peers, "-replication-factor", "$ReplicationFactor")
    if ($Raft) { $argsCommon += "-raft" }

    foreach ($node in @(
        @{ Id = "node-a"; Port = 6379 },
        @{ Id = "node-b"; Port = 6380 },
        @{ Id = "node-c"; Port = 6381 }
    )) {
        $args = @("-addr", ":$($node.Port)", "-node-id", $node.Id, "-advertise-addr", "127.0.0.1:$($node.Port)") + $argsCommon
        $procs += Start-Process -FilePath $NodeBin -ArgumentList $args -PassThru -WindowStyle Hidden
        Start-Sleep -Milliseconds 750
    }
    Wait-Port "127.0.0.1:6379" -TimeoutSec 30
    if ($Raft) { Start-Sleep -Seconds 5 }
    return $procs
}

function Test-DockerAvailable {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    try {
        docker info 1>$null 2>$null
        return ($LASTEXITCODE -eq 0)
    } finally {
        $ErrorActionPreference = $prev
    }
}

function Run-DockerCluster {
    param(
        [string]$Name,
        [string]$Label,
        [string[]]$ComposeArgs
    )

    docker compose @ComposeArgs down -v 2>$null | Out-Null
    docker compose @ComposeArgs up -d --build --wait
    try {
        Wait-Port "127.0.0.1:6379" -TimeoutSec 120
        Start-Sleep -Seconds 3
        return Invoke-LoadTest -Name $Name -Addr "127.0.0.1:6379" -ExtraArgs @("-addr-map=$AddrMap")
    } finally {
        docker compose @ComposeArgs down -v | Out-Null
    }
}

function Run-LocalClusterBenchmark {
    param(
        [string]$Name,
        [int]$ReplicationFactor,
        [switch]$Raft
    )

    $procs = Start-LocalCluster -ReplicationFactor $ReplicationFactor -Raft:$Raft
    try {
        return Invoke-LoadTest -Name $Name -Addr "127.0.0.1:6379"
    } finally {
        Stop-Procs $procs
    }
}

# A) Single node
$singleProc = Start-Process -FilePath $NodeBin -ArgumentList @("-addr", ":16379") -PassThru -WindowStyle Hidden
try {
    Wait-Port "127.0.0.1:16379"
    $single = Invoke-LoadTest -Name "single-node" -Addr "127.0.0.1:16379"
} finally {
    Stop-Procs @($singleProc)
}

$rf1 = $null
$rf3 = $null
$clusterMode = "none"

if (-not $SkipCluster) {
    if (Test-DockerAvailable) {
        $clusterMode = "docker"
        Write-Host "Docker available; running cluster benchmarks via Compose." -ForegroundColor Yellow
        $rf1 = Run-DockerCluster -Name "cluster-rf1-docker" -Label "Docker cluster RF=1" -ComposeArgs @("-f", $ComposeFile, "-f", $ComposeRF1)
        $rf3 = Run-DockerCluster -Name "cluster-rf3-docker" -Label "Docker cluster RF=3 (W=2 R=2)" -ComposeArgs @("-f", $ComposeFile)
    } else {
        $clusterMode = "local"
        Write-Warning "Docker unavailable; running local 3-node cluster with equivalent Compose settings."
        $rf1 = Run-LocalClusterBenchmark -Name "cluster-rf1-local" -ReplicationFactor 1
        $rf3 = Run-LocalClusterBenchmark -Name "cluster-rf3-local" -ReplicationFactor 3 -Raft
    }
}

function Format-Nanos([double]$ns) {
    if ($ns -le 0) { return "<1us" }
    $us = $ns / 1000.0
    if ($us -lt 1000) {
        return ("{0:N0}us" -f $us)
    }
    return ("{0:N2}ms" -f ($ns / 1000000.0))
}

function Format-Row($label, $result) {
    if ($null -eq $result) { return }
    return "| $label | {0:N0} ops/sec | {1} | {2} | {3} | {4} |" -f `
        $result.throughput_ops_per_sec, `
        (Format-Nanos $result.latency.p50), `
        (Format-Nanos $result.latency.p95), `
        (Format-Nanos $result.latency.p99), `
        $result.errors
}

$rf1Label = if ($clusterMode -eq "docker") { "Docker cluster RF=1" } else { "Cluster RF=1 (local 3-node)" }
$rf3Label = if ($clusterMode -eq "docker") { "Docker cluster RF=3 (W=2 R=2)" } else { "Cluster RF=3 W=2 R=2 (local 3-node)" }

$table = @(
    "| Scenario | Throughput | p50 | p95 | p99 | Errors |",
    "| --- | --- | --- | --- | --- | --- |"
)
$table += Format-Row "Single node" $single
if ($null -ne $rf1) { $table += Format-Row $rf1Label $rf1 }
if ($null -ne $rf3) { $table += Format-Row $rf3Label $rf3 }

$meta = @(
    "",
    "Measured: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "Duration: $Duration, concurrency: $Concurrency, get-ratio: 0.8",
    "Cluster mode: $clusterMode"
)
$tableText = ($table + $meta) -join "`n"
$tablePath = Join-Path $RunDir "matrix.md"
$tableText | Out-File -FilePath $tablePath -Encoding utf8

Write-Host "`n=== Benchmark Matrix ===" -ForegroundColor Green
Write-Host $tableText
Write-Host "`nResults saved to $RunDir"
