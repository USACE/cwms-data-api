param([Parameter(Mandatory=$true)][string]$ServerDirectory)
$ready = Get-Content -Raw -LiteralPath (Join-Path $ServerDirectory 'ready.json') | ConvertFrom-Json
$processId = [int]$ready.pid
$description = Get-CimInstance Win32_Process -Filter "ProcessId=$processId"
if ($description.CommandLine -notmatch 'helpers.TimeSeriesReadBenchmark') {
    throw 'The ready file does not identify an owned benchmark Java process'
}
$apiProcess = Get-Process -Id $processId -ErrorAction Stop
$apiProcess.ProcessorAffinity = [IntPtr]3
$apiProcess.Refresh()
$settings = @{
    pid=$processId
    affinityMask=$apiProcess.ProcessorAffinity.ToInt64()
    jvmVisibleProcessors=$ready.processors
    hostLogicalProcessors=[Environment]::ProcessorCount
    note='API fixture JVM restricted to two Windows logical processors. No 4 GiB process memory limit is imposed.'
}
$settings | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $ServerDirectory 'process-limits.json') -Encoding UTF8
$writer = [System.IO.StreamWriter]::new((Join-Path $ServerDirectory 'process-memory.csv'), $false,
    [System.Text.UTF8Encoding]::new($false))
try {
    $writer.WriteLine('epochMs,workingSetBytes,privateBytes,virtualBytes,cpuMs,affinityMask')
    while (-not $apiProcess.HasExited) {
        $apiProcess.Refresh()
        $writer.WriteLine(('{0},{1},{2},{3},{4},{5}' -f
            [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds(), $apiProcess.WorkingSet64,
            $apiProcess.PrivateMemorySize64, $apiProcess.VirtualMemorySize64,
            $apiProcess.TotalProcessorTime.TotalMilliseconds, $apiProcess.ProcessorAffinity.ToInt64()))
        $writer.Flush()
        Start-Sleep -Milliseconds 500
    }
} finally {
    $writer.Dispose()
}
