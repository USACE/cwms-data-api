[CmdletBinding()]
param(
    [string]$Office = "SPK",
    [string]$LocationId = "PERF1MREAD",
    [string]$SeriesId = "PERF1MREAD.Stage.Inst.1Minute.0.BENCH",
    [string]$Units = "ft",
    [string]$CdaBaseUrl = "http://localhost:8081/cwms-data",
    [string]$DbContainer = "cwms-data-api-db-1",
    [string]$DbUser = "CWMS_20",
    [string]$DbPassword = "simplecwmspasswD1",
    [string]$DbService = "localhost:1521/FREEPDB1",
    [string]$StartTime = "2024-01-01T00:00:00Z",
    [int]$PointCount = 1000000,
    [int]$PageSize = 1000000,
    [int]$Runs = 1,
    [switch]$Warmup,
    [switch]$SkipSeed,
    [switch]$ForceReseed,
    [switch]$KeepResponses
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$SqlPlusPath = "/opt/oracle/product/23ai/dbhomeFree/bin/sqlplus"
$ResultsDir = Join-Path $PSScriptRoot "results"
$ResponsesDir = Join-Path $PSScriptRoot "responses"
$NonVersionedDateSql = "date '1111-11-11'"

function Convert-ToSqlStringLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Convert-ToOracleDateExpression {
    param([datetimeoffset]$Value)
    $utc = $Value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")
    return "to_date('$utc', 'yyyy-mm-dd hh24:mi:ss')"
}

function Invoke-OracleSql {
    param(
        [string]$Sql,
        [string]$Label = "oracle"
    )

    $sqlFile = Join-Path $env:TEMP ("cwms-benchmark-{0}-{1}.sql" -f $Label, [guid]::NewGuid().ToString("N"))
    try {
        Set-Content -LiteralPath $sqlFile -Value $Sql -Encoding ASCII

        $containerSqlFile = "/tmp/" + [System.IO.Path]::GetFileName($sqlFile)
        $null = & docker cp $sqlFile "${DbContainer}:${containerSqlFile}"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy SQL to container $DbContainer"
        }

        $command = "$SqlPlusPath -s -L $DbUser/$DbPassword@$DbService @$containerSqlFile"
        $output = & docker exec $DbContainer bash -lc $command 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw ("Oracle SQL failed for {0}:`n{1}" -f $Label, ($output -join [Environment]::NewLine))
        }

        return ($output -join [Environment]::NewLine)
    }
    finally {
        if (Test-Path -LiteralPath $sqlFile) {
            Remove-Item -LiteralPath $sqlFile -Force
        }
    }
}

function Get-YearSegments {
    param(
        [datetimeoffset]$StartUtc,
        [int]$Count
    )

    $segments = @()
    $remaining = $Count
    $offset = 0
    $cursor = $StartUtc.ToUniversalTime()

    while ($remaining -gt 0) {
        $yearStart = [datetimeoffset]::ParseExact(
            "{0}-01-01T00:00:00+00:00" -f $cursor.Year,
            "yyyy-MM-ddTHH:mm:sszzz",
            [System.Globalization.CultureInfo]::InvariantCulture
        )
        $nextYear = $yearStart.AddYears(1)
        $minutesUntilNextYear = [int][Math]::Floor(($nextYear - $cursor).TotalMinutes)
        if ($minutesUntilNextYear -le 0) {
            throw "Computed non-positive year segment size for $($cursor.Year)"
        }

        $segmentCount = [Math]::Min($remaining, $minutesUntilNextYear)
        $segments += [pscustomobject]@{
            Year       = $cursor.Year
            Start      = $cursor
            Count      = $segmentCount
            ValueStart = $offset + 1
        }

        $cursor = $cursor.AddMinutes($segmentCount)
        $remaining -= $segmentCount
        $offset += $segmentCount
    }

    return $segments
}

function Get-SeededPointCount {
    $seriesLiteral = Convert-ToSqlStringLiteral $SeriesId
    $officeLiteral = Convert-ToSqlStringLiteral $Office
    $sql = @"
set heading off feedback off verify off pagesize 0 trimspool on
select count(*)
  from av_tsv v
  join at_cwms_ts_id t
    on t.ts_code = v.ts_code
 where t.db_office_id = $officeLiteral
   and t.cwms_ts_id = $seriesLiteral;
exit;
"@

    $raw = Invoke-OracleSql -Sql $sql -Label "count"
    $countText = (($raw -split "\r?\n") | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Last 1)
    return [int]$countText
}

function Ensure-BenchmarkSeed {
    param(
        [datetimeoffset]$StartUtc,
        [int]$Count
    )

    if ($SkipSeed) {
        return [pscustomobject]@{
            Seeded = $false
            ExistingPointCount = Get-SeededPointCount
        }
    }

    $existingCount = Get-SeededPointCount
    if (-not $ForceReseed -and $existingCount -eq $Count) {
        return [pscustomobject]@{
            Seeded = $false
            ExistingPointCount = $existingCount
        }
    }

    $seriesLiteral = Convert-ToSqlStringLiteral $SeriesId
    $locationLiteral = Convert-ToSqlStringLiteral $LocationId
    $officeLiteral = Convert-ToSqlStringLiteral $Office
    $locationTypeLiteral = Convert-ToSqlStringLiteral "SITE"
    $publicNameLiteral = Convert-ToSqlStringLiteral $LocationId
    $longNameLiteral = Convert-ToSqlStringLiteral "$LocationId Benchmark Location"
    $descriptionLiteral = Convert-ToSqlStringLiteral "Performance benchmark location"
    $timeZoneLiteral = Convert-ToSqlStringLiteral "UTC"
    $horizontalDatumLiteral = Convert-ToSqlStringLiteral "NAD83"
    $segments = Get-YearSegments -StartUtc $StartUtc -Count $Count

    $insertStatements = foreach ($segment in $segments) {
        $dateExpr = Convert-ToOracleDateExpression $segment.Start
        @"
  execute immediate q'[
    insert /*+ APPEND */ into at_tsv_$($segment.Year)
      (ts_code, date_time, version_date, data_entry_date, value, quality_code, dest_flag)
    select :1,
           $dateExpr + numtodsinterval(level - 1, 'MINUTE'),
           $NonVersionedDateSql,
           systimestamp,
           $($segment.ValueStart) + level - 1,
           0,
           0
      from dual
   connect by level <= $($segment.Count)
  ]' using l_ts_code;
"@
    }

    $seedSql = @"
set serveroutput on feedback on
whenever sqlerror exit failure rollback
declare
  location_exists exception;
  pragma exception_init(location_exists, -20026);
  ts_exists exception;
  pragma exception_init(ts_exists, -20003);
  l_ts_code number;
begin
  begin
    cwms_loc.create_location(
      p_location_id      => $locationLiteral,
      p_location_type    => $locationTypeLiteral,
      p_elevation        => null,
      p_elev_unit_id     => null,
      p_vertical_datum   => null,
      p_latitude         => 38.0,
      p_longitude        => -90.0,
      p_horizontal_datum => $horizontalDatumLiteral,
      p_public_name      => $publicNameLiteral,
      p_long_name        => $longNameLiteral,
      p_description      => $descriptionLiteral,
      p_time_zone_id     => $timeZoneLiteral,
      p_county_name      => null,
      p_state_initial    => null,
      p_active           => 'T',
      p_db_office_id     => $officeLiteral
    );
  exception
    when location_exists then null;
  end;

  begin
    cwms_ts.create_ts($officeLiteral, $seriesLiteral, 0);
  exception
    when ts_exists then null;
  end;

  select ts_code
    into l_ts_code
    from at_cwms_ts_id
   where db_office_id = $officeLiteral
     and cwms_ts_id = $seriesLiteral;

  for rec in (select table_name from at_ts_table_properties) loop
    execute immediate 'delete from ' || rec.table_name || ' where ts_code = :1' using l_ts_code;
  end loop;

  delete from at_ts_extents where ts_code = l_ts_code;

$($insertStatements -join [Environment]::NewLine)

  cwms_ts.update_ts_extents(l_ts_code, $NonVersionedDateSql);
  commit;
end;
/
set heading off feedback off verify off pagesize 0 trimspool on
select count(*)
  from av_tsv v
  join at_cwms_ts_id t
    on t.ts_code = v.ts_code
 where t.db_office_id = $officeLiteral
   and t.cwms_ts_id = $seriesLiteral;
exit;
"@

    $raw = Invoke-OracleSql -Sql $seedSql -Label "seed"
    $countText = (($raw -split "\r?\n") | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Last 1)
    return [pscustomobject]@{
        Seeded = $true
        ExistingPointCount = [int]$countText
    }
}

function Invoke-CdaRequest {
    param(
        [string]$Url,
        [string]$ResponseFile
    )

    $format = '{"http_code":%{http_code},"time_total":%{time_total},"time_starttransfer":%{time_starttransfer},"time_connect":%{time_connect},"size_download":%{size_download},"speed_download":%{speed_download}}'
    $json = & curl.exe -sS -H "Accept: application/json;version=2" -o $ResponseFile -w $format $Url 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("curl failed: {0}" -f ($json -join [Environment]::NewLine))
    }

    return ($json | ConvertFrom-Json)
}

function Wait-ForCdaReady {
    param(
        [string]$Url,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 1
    )

    $probeFile = Join-Path $ResponsesDir "readiness-probe.json"
    try {
        for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
            if (Test-Path -LiteralPath $probeFile) {
                Remove-Item -LiteralPath $probeFile -Force
            }

            $response = Invoke-CdaRequest -Url $Url -ResponseFile $probeFile
            if ($response.http_code -eq 200) {
                return
            }

            Start-Sleep -Seconds $DelaySeconds
        }
    }
    finally {
        if (Test-Path -LiteralPath $probeFile) {
            Remove-Item -LiteralPath $probeFile -Force
        }
    }

    throw "CDA did not become ready after $MaxAttempts attempts: $Url"
}

function Get-ResponseSummary {
    param([string]$ResponseFile)

    $content = Get-Content -LiteralPath $ResponseFile -Raw
    $total = $null
    $pageSize = $null
    $firstTimestamp = $null
    $lastTimestamp = $null

    if ($content -match '"total":(?<total>\d+)') {
        $total = [int]$Matches["total"]
    }
    if ($content -match '"page-size":(?<pageSize>\d+)') {
        $pageSize = [int]$Matches["pageSize"]
    }
    if ($content -match '\[\[(?<first>\d+),') {
        $firstTimestamp = [long]$Matches["first"]
    }
    $allMatches = [regex]::Matches($content, '\[(?<ts>\d+),')
    if ($allMatches.Count -gt 0) {
        $lastTimestamp = [long]$allMatches[$allMatches.Count - 1].Groups["ts"].Value
    }

    return [pscustomobject]@{
        Total = $total
        PageSize = $pageSize
        FirstTimestamp = $firstTimestamp
        LastTimestamp = $lastTimestamp
        ResponseBytes = (Get-Item -LiteralPath $ResponseFile).Length
    }
}

$startUtc = [datetimeoffset]::Parse($StartTime, [System.Globalization.CultureInfo]::InvariantCulture).ToUniversalTime()
$endUtc = $startUtc.AddMinutes($PointCount - 1)
$escapedSeriesId = [uri]::EscapeDataString($SeriesId)
$escapedOffice = [uri]::EscapeDataString($Office)
$escapedUnits = [uri]::EscapeDataString($Units)
$escapedBegin = [uri]::EscapeDataString($startUtc.ToString("yyyy-MM-ddTHH:mm:ssZ"))
$escapedEnd = [uri]::EscapeDataString($endUtc.ToString("yyyy-MM-ddTHH:mm:ssZ"))
$requestUrl = "{0}/timeseries?office={1}&name={2}&units={3}&begin={4}&end={5}&page-size={6}" -f `
    $CdaBaseUrl.TrimEnd("/"), `
    $escapedOffice, `
    $escapedSeriesId, `
    $escapedUnits, `
    $escapedBegin, `
    $escapedEnd, `
    $PageSize

New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null
New-Item -ItemType Directory -Path $ResponsesDir -Force | Out-Null

$seedInfo = Ensure-BenchmarkSeed -StartUtc $startUtc -Count $PointCount
if ($seedInfo.ExistingPointCount -ne $PointCount) {
    throw "Expected $PointCount seeded points but found $($seedInfo.ExistingPointCount)"
}

Wait-ForCdaReady -Url ("{0}/offices/{1}" -f $CdaBaseUrl.TrimEnd("/"), $escapedOffice)

if ($Warmup) {
    $warmupFile = Join-Path $ResponsesDir "warmup.json"
    $null = Invoke-CdaRequest -Url $requestUrl -ResponseFile $warmupFile
    if (-not $KeepResponses -and (Test-Path -LiteralPath $warmupFile)) {
        Remove-Item -LiteralPath $warmupFile -Force
    }
}

$results = @()
$failedRuns = @()
for ($run = 1; $run -le $Runs; $run++) {
    $responseFile = Join-Path $ResponsesDir ("timeseries-read-run-{0}.json" -f $run)
    $curlMetrics = Invoke-CdaRequest -Url $requestUrl -ResponseFile $responseFile
    $responseSummary = Get-ResponseSummary -ResponseFile $responseFile
    $errorBody = $null
    if ($curlMetrics.http_code -ne 200) {
        $errorBody = [string](Get-Content -LiteralPath $responseFile -Raw)
    }

    $result = [pscustomobject]@{
        run = $run
        http_code = [int]$curlMetrics.http_code
        time_total_seconds = [double]$curlMetrics.time_total
        time_starttransfer_seconds = [double]$curlMetrics.time_starttransfer
        time_connect_seconds = [double]$curlMetrics.time_connect
        size_download_bytes = [double]$curlMetrics.size_download
        speed_download_bytes_per_second = [double]$curlMetrics.speed_download
        response_bytes_on_disk = [long]$responseSummary.ResponseBytes
        reported_total = $responseSummary.Total
        reported_page_size = $responseSummary.PageSize
        first_timestamp = $responseSummary.FirstTimestamp
        last_timestamp = $responseSummary.LastTimestamp
        error_body = $errorBody
        response_file = $responseFile
    }
    $results += $result
    if ($curlMetrics.http_code -ne 200) {
        $failedRuns += $result
    }

    if (-not $KeepResponses -and (Test-Path -LiteralPath $responseFile)) {
        Remove-Item -LiteralPath $responseFile -Force
        $result.response_file = $null
    }
}

$gitBranch = (& git branch --show-current 2>$null)
$gitBranchExitCode = $LASTEXITCODE
$gitCommit = (& git rev-parse HEAD 2>$null)
$gitCommitExitCode = $LASTEXITCODE
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$resultFile = Join-Path $ResultsDir ("timeseries-read-benchmark-{0}.json" -f $timestamp)
$successfulRuns = @($results | Where-Object { $_.http_code -eq 200 })
$summary = $null
if ($successfulRuns.Count -gt 0) {
    $avg = ($successfulRuns | Measure-Object -Property time_total_seconds -Average).Average
    $min = ($successfulRuns | Measure-Object -Property time_total_seconds -Minimum).Minimum
    $max = ($successfulRuns | Measure-Object -Property time_total_seconds -Maximum).Maximum
    $summary = [pscustomobject]@{
        successful_runs = $successfulRuns.Count
        average_time_total_seconds = [math]::Round([double]$avg, 6)
        min_time_total_seconds = [math]::Round([double]$min, 6)
        max_time_total_seconds = [math]::Round([double]$max, 6)
    }
}

$payload = [pscustomobject]@{
    benchmark = "timeseries-read"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    git_branch = if ($gitBranchExitCode -eq 0) { $gitBranch.Trim() } else { $null }
    git_commit = if ($gitCommitExitCode -eq 0) { $gitCommit.Trim() } else { $null }
    office = $Office
    location_id = $LocationId
    series_id = $SeriesId
    units = $Units
    start_time_utc = $startUtc.ToString("o")
    end_time_utc = $endUtc.ToString("o")
    point_count = $PointCount
    page_size = $PageSize
    request_url = $requestUrl
    seed = [pscustomobject]@{
        seeded = [bool]$seedInfo.Seeded
        point_count = [int]$seedInfo.ExistingPointCount
    }
    summary = $summary
    runs = $results
}

$payload | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $resultFile -Encoding ASCII
$payload | ConvertTo-Json -Depth 6

if ($failedRuns.Count -gt 0) {
    $statusList = ($failedRuns | ForEach-Object { $_.http_code }) -join ", "
    throw "Benchmark completed with HTTP failures ($statusList). Results saved to $resultFile"
}
