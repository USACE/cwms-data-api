[CmdletBinding()]
param(
    [string]$Office = "SPK",
    [string]$CdaBaseUrl = "http://localhost:8081/cwms-data",
    [string]$DbContainer = "cwms-data-api-db-1",
    [string]$DbUser = "CWMS_20",
    [string]$DbPassword = "simplecwmspasswD1",
    [string]$DbService = "localhost:1521/FREEPDB1",
    [string[]]$Scenarios = @(
        "dense-regular",
        "dense-regular-entry-date",
        "gap-regular",
        "versioned-max",
        "versioned-single",
        "irregular"
    ),
    [switch]$KeepResponses
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$SqlPlusPath = "/opt/oracle/product/23ai/dbhomeFree/bin/sqlplus"
$ResultsDir = Join-Path $PSScriptRoot "results"
$ResponsesDir = Join-Path $PSScriptRoot "responses"
$NonVersionedDateSql = "date '1111-11-11'"
$FloatTolerance = 1e-9

function Convert-ToSqlStringLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Convert-ToOracleDateExpression {
    param([datetimeoffset]$Value)
    $utc = $Value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")
    return "to_date('$utc', 'yyyy-mm-dd hh24:mi:ss')"
}

function Convert-ToOracleTimestampExpression {
    param([datetimeoffset]$Value)
    $utc = $Value.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")
    return "to_timestamp('$utc', 'yyyy-mm-dd hh24:mi:ss')"
}

function Invoke-OracleSql {
    param(
        [string]$Sql,
        [string]$Label = "oracle"
    )

    $sqlFile = Join-Path $env:TEMP ("cwms-parity-{0}-{1}.sql" -f $Label, [guid]::NewGuid().ToString("N"))
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

    $probeFile = Join-Path $ResponsesDir "parity-readiness-probe.json"
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

function New-SeedRow {
    param(
        [datetimeoffset]$DateTime,
        [double]$Value,
        [int]$QualityCode = 0,
        [datetimeoffset]$DataEntryDate,
        [Nullable[datetimeoffset]]$VersionDate = $null
    )

    return [pscustomobject]@{
        DateTime = $DateTime.ToUniversalTime()
        Value = $Value
        QualityCode = $QualityCode
        DataEntryDate = $DataEntryDate.ToUniversalTime()
        VersionDate = $VersionDate
    }
}

function New-Scenario {
    param(
        [string]$Name,
        [string]$LocationId,
        [string]$SeriesId,
        [string]$Units,
        [datetimeoffset]$BeginTime,
        [datetimeoffset]$EndTime,
        [object[]]$Rows,
        [bool]$Versioned,
        [bool]$IncludeEntryDate,
        [string]$ExpectedDateVersionType,
        [long]$ExpectedIntervalOffset,
        [string]$ExpectedInterval,
        [Nullable[datetimeoffset]]$VersionDate = $null
    )

    return [pscustomobject]@{
        Name = $Name
        LocationId = $LocationId
        SeriesId = $SeriesId
        Units = $Units
        BeginTime = $BeginTime.ToUniversalTime()
        EndTime = $EndTime.ToUniversalTime()
        Rows = $Rows
        Versioned = $Versioned
        IncludeEntryDate = $IncludeEntryDate
        ExpectedDateVersionType = $ExpectedDateVersionType
        ExpectedIntervalOffset = $ExpectedIntervalOffset
        ExpectedInterval = $ExpectedInterval
        VersionDate = $VersionDate
    }
}

function Get-ScenarioDefinitions {
    $denseRows = @(
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:00:00Z") -Value 1 -DataEntryDate ([datetimeoffset]"2024-01-02T00:00:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:01:00Z") -Value 2 -DataEntryDate ([datetimeoffset]"2024-01-02T00:01:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:02:00Z") -Value 3 -DataEntryDate ([datetimeoffset]"2024-01-02T00:02:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:03:00Z") -Value 4 -DataEntryDate ([datetimeoffset]"2024-01-02T00:03:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:04:00Z") -Value 5 -DataEntryDate ([datetimeoffset]"2024-01-02T00:04:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:05:00Z") -Value 6 -DataEntryDate ([datetimeoffset]"2024-01-02T00:05:00Z"))
    )

    $gapRows = @(
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:00:00Z") -Value 1 -DataEntryDate ([datetimeoffset]"2024-01-03T00:00:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:01:00Z") -Value 2 -DataEntryDate ([datetimeoffset]"2024-01-03T00:01:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:02:00Z") -Value 3 -DataEntryDate ([datetimeoffset]"2024-01-03T00:02:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:05:00Z") -Value 6 -DataEntryDate ([datetimeoffset]"2024-01-03T00:05:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:06:00Z") -Value 7 -DataEntryDate ([datetimeoffset]"2024-01-03T00:06:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:07:00Z") -Value 8 -DataEntryDate ([datetimeoffset]"2024-01-03T00:07:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:08:00Z") -Value 9 -DataEntryDate ([datetimeoffset]"2024-01-03T00:08:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-01T00:09:00Z") -Value 10 -DataEntryDate ([datetimeoffset]"2024-01-03T00:09:00Z"))
    )

    $versionDateOlder = [datetimeoffset]"2024-06-20T08:00:00Z"
    $versionDateNewer = [datetimeoffset]"2024-06-21T08:00:00Z"
    $versionedRows = @(
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T15:00:00Z") -Value 4 -DataEntryDate ([datetimeoffset]"2024-06-20T09:00:00Z") -VersionDate $versionDateOlder),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T16:00:00Z") -Value 4 -DataEntryDate ([datetimeoffset]"2024-06-20T09:01:00Z") -VersionDate $versionDateOlder),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T17:00:00Z") -Value 4 -DataEntryDate ([datetimeoffset]"2024-06-20T09:02:00Z") -VersionDate $versionDateOlder),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T18:00:00Z") -Value 3 -DataEntryDate ([datetimeoffset]"2024-06-20T09:03:00Z") -VersionDate $versionDateOlder),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T15:00:00Z") -Value 1 -DataEntryDate ([datetimeoffset]"2024-06-21T09:00:00Z") -VersionDate $versionDateNewer),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T16:00:00Z") -Value 1 -DataEntryDate ([datetimeoffset]"2024-06-21T09:01:00Z") -VersionDate $versionDateNewer),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-05-01T17:00:00Z") -Value 1 -DataEntryDate ([datetimeoffset]"2024-06-21T09:02:00Z") -VersionDate $versionDateNewer)
    )

    $irregularRows = @(
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-05T12:00:00Z") -Value 10 -DataEntryDate ([datetimeoffset]"2024-01-06T00:00:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-05T12:07:20Z") -Value 20 -DataEntryDate ([datetimeoffset]"2024-01-06T00:01:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-05T12:19:45Z") -Value 30 -DataEntryDate ([datetimeoffset]"2024-01-06T00:02:00Z")),
        (New-SeedRow -DateTime ([datetimeoffset]"2024-01-05T12:33:10Z") -Value 40 -DataEntryDate ([datetimeoffset]"2024-01-06T00:03:00Z"))
    )

    return @(
        (New-Scenario -Name "dense-regular" -LocationId "PARREG" -SeriesId "PARREG.Stage.Inst.1Minute.0.BENCH" -Units "ft" -BeginTime ([datetimeoffset]"2024-01-01T00:00:00Z") -EndTime ([datetimeoffset]"2024-01-01T00:05:00Z") -Rows $denseRows -Versioned $false -IncludeEntryDate $false -ExpectedDateVersionType "UNVERSIONED" -ExpectedIntervalOffset 0 -ExpectedInterval "PT1M"),
        (New-Scenario -Name "dense-regular-entry-date" -LocationId "PARREG" -SeriesId "PARREG.Stage.Inst.1Minute.0.BENCH" -Units "ft" -BeginTime ([datetimeoffset]"2024-01-01T00:00:00Z") -EndTime ([datetimeoffset]"2024-01-01T00:05:00Z") -Rows $denseRows -Versioned $false -IncludeEntryDate $true -ExpectedDateVersionType "UNVERSIONED" -ExpectedIntervalOffset 0 -ExpectedInterval "PT1M"),
        (New-Scenario -Name "gap-regular" -LocationId "PARGAP" -SeriesId "PARGAP.Stage.Inst.1Minute.0.BENCH" -Units "ft" -BeginTime ([datetimeoffset]"2024-01-01T00:00:00Z") -EndTime ([datetimeoffset]"2024-01-01T00:09:00Z") -Rows $gapRows -Versioned $false -IncludeEntryDate $false -ExpectedDateVersionType "UNVERSIONED" -ExpectedIntervalOffset 0 -ExpectedInterval "PT1M"),
        (New-Scenario -Name "versioned-max" -LocationId "PARVER" -SeriesId "PARVER.Flow.Inst.1Hour.0.BENCH" -Units "cfs" -BeginTime ([datetimeoffset]"2024-05-01T15:00:00Z") -EndTime ([datetimeoffset]"2024-05-01T18:00:00Z") -Rows $versionedRows -Versioned $true -IncludeEntryDate $false -ExpectedDateVersionType "MAX_AGGREGATE" -ExpectedIntervalOffset 0 -ExpectedInterval "PT1H"),
        (New-Scenario -Name "versioned-single" -LocationId "PARVER" -SeriesId "PARVER.Flow.Inst.1Hour.0.BENCH" -Units "cfs" -BeginTime ([datetimeoffset]"2024-05-01T15:00:00Z") -EndTime ([datetimeoffset]"2024-05-01T18:00:00Z") -Rows $versionedRows -Versioned $true -IncludeEntryDate $false -ExpectedDateVersionType "SINGLE_VERSION" -ExpectedIntervalOffset 0 -ExpectedInterval "PT1H" -VersionDate $versionDateNewer),
        (New-Scenario -Name "irregular" -LocationId "PARIRR" -SeriesId "PARIRR.Flow.Inst.0.0.BENCH" -Units "cfs" -BeginTime ([datetimeoffset]"2024-01-05T12:00:00Z") -EndTime ([datetimeoffset]"2024-01-05T12:33:10Z") -Rows $irregularRows -Versioned $false -IncludeEntryDate $false -ExpectedDateVersionType "UNVERSIONED" -ExpectedIntervalOffset (-2147483648L) -ExpectedInterval "PT0S")
    )
}

function Convert-SeedValueToSqlLiteral {
    param([double]$Value)
    return ([System.Globalization.CultureInfo]::InvariantCulture.TextInfo.ToLower($Value.ToString("0.################", [System.Globalization.CultureInfo]::InvariantCulture)))
}

function Get-SeedSql {
    param($Scenario)

    $seriesLiteral = Convert-ToSqlStringLiteral $Scenario.SeriesId
    $locationLiteral = Convert-ToSqlStringLiteral $Scenario.LocationId
    $officeLiteral = Convert-ToSqlStringLiteral $Office
    $locationTypeLiteral = Convert-ToSqlStringLiteral "SITE"
    $publicNameLiteral = Convert-ToSqlStringLiteral $Scenario.LocationId
    $longNameLiteral = Convert-ToSqlStringLiteral "$($Scenario.LocationId) Parity Location"
    $descriptionLiteral = Convert-ToSqlStringLiteral "Parity harness location"
    $timeZoneLiteral = Convert-ToSqlStringLiteral "UTC"
    $horizontalDatumLiteral = Convert-ToSqlStringLiteral "NAD83"
    $versionedFlagLiteral = if ($Scenario.Versioned) { "'T'" } else { "'F'" }

    $groupedRows = $Scenario.Rows | Group-Object { $_.DateTime.Year }
    $insertStatements = foreach ($group in $groupedRows) {
        $intoStatements = foreach ($row in $group.Group) {
            $dateExpr = Convert-ToOracleDateExpression $row.DateTime
            $versionExpr = if ($null -ne $row.VersionDate) {
                Convert-ToOracleDateExpression $row.VersionDate
            } else {
                $NonVersionedDateSql
            }
            $entryExpr = Convert-ToOracleTimestampExpression $row.DataEntryDate
            $valueExpr = Convert-SeedValueToSqlLiteral $row.Value
            "  into at_tsv_$($group.Name) (ts_code, date_time, version_date, data_entry_date, value, quality_code, dest_flag) values (l_ts_code, $dateExpr, $versionExpr, $entryExpr, $valueExpr, $($row.QualityCode), 0)"
        }

        @"
insert all
$($intoStatements -join [Environment]::NewLine)
select 1 from dual;
"@
    }

    $distinctVersionDates = @($Scenario.Rows |
        ForEach-Object { $_.VersionDate } |
        Where-Object { $null -ne $_ } |
        Sort-Object |
        Get-Unique)

    $extentStatements = if ($distinctVersionDates.Count -gt 0) {
        foreach ($versionDate in $distinctVersionDates) {
            "  cwms_ts.update_ts_extents(l_ts_code, $(Convert-ToOracleDateExpression $versionDate));"
        }
    } else {
        "  cwms_ts.update_ts_extents(l_ts_code, $NonVersionedDateSql);"
    }

    return @"
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

  cwms_ts.set_tsid_versioned($seriesLiteral, $versionedFlagLiteral, $officeLiteral);

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

$($extentStatements -join [Environment]::NewLine)
  commit;
end;
/
exit;
"@
}

function Convert-CdaResponseToRows {
    param(
        [object]$Payload,
        [bool]$IncludeEntryDate
    )

    $rows = @()
    foreach ($entry in $Payload.values) {
        $row = [ordered]@{
            date_time = [long]$entry[0]
            value = if ($null -eq $entry[1]) { $null } else { [double]$entry[1] }
            quality_code = [int]$entry[2]
        }
        if ($IncludeEntryDate) {
            $row.data_entry_date = if ($entry.Count -gt 3 -and $null -ne $entry[3]) {
                [long]$entry[3]
            } else {
                $null
            }
        }
        $rows += [pscustomobject]$row
    }

    return @($rows | Sort-Object date_time)
}

function Get-CdaScenarioResult {
    param($Scenario)

    $responseFile = Join-Path $ResponsesDir ("parity-{0}-cda.json" -f $Scenario.Name)
    $escapedOffice = [uri]::EscapeDataString($Office)
    $escapedSeriesId = [uri]::EscapeDataString($Scenario.SeriesId)
    $escapedUnits = [uri]::EscapeDataString($Scenario.Units)
    $escapedBegin = [uri]::EscapeDataString($Scenario.BeginTime.ToString("yyyy-MM-ddTHH:mm:ssZ"))
    $escapedEnd = [uri]::EscapeDataString($Scenario.EndTime.ToString("yyyy-MM-ddTHH:mm:ssZ"))
    $requestUrl = "{0}/timeseries?office={1}&name={2}&units={3}&begin={4}&end={5}&page-size=1000" -f `
        $CdaBaseUrl.TrimEnd("/"), `
        $escapedOffice, `
        $escapedSeriesId, `
        $escapedUnits, `
        $escapedBegin, `
        $escapedEnd

    if ($Scenario.IncludeEntryDate) {
        $requestUrl += "&include-entry-date=true"
    }

    if ($null -ne $Scenario.VersionDate) {
        $escapedVersionDate = [uri]::EscapeDataString($Scenario.VersionDate.ToString("yyyy-MM-ddTHH:mm:ssZ"))
        $requestUrl += "&version-date=$escapedVersionDate"
    }

    $curlMetrics = Invoke-CdaRequest -Url $requestUrl -ResponseFile $responseFile
    $payload = Get-Content -LiteralPath $responseFile -Raw | ConvertFrom-Json
    $rows = Convert-CdaResponseToRows -Payload $payload -IncludeEntryDate $Scenario.IncludeEntryDate

    if (-not $KeepResponses -and (Test-Path -LiteralPath $responseFile)) {
        Remove-Item -LiteralPath $responseFile -Force
        $responseFile = $null
    }

    return [pscustomobject]@{
        RequestUrl = $requestUrl
        HttpCode = [int]$curlMetrics.http_code
        TimeTotalSeconds = [double]$curlMetrics.time_total
        Payload = $payload
        Rows = $rows
        ResponseFile = $responseFile
    }
}

function Get-OracleRowsSql {
    param($Scenario)

    $seriesLiteral = Convert-ToSqlStringLiteral $Scenario.SeriesId
    $unitsLiteral = Convert-ToSqlStringLiteral $Scenario.Units
    $officeLiteral = Convert-ToSqlStringLiteral $Office
    $beginExpr = Convert-ToOracleDateExpression $Scenario.BeginTime
    $endExpr = Convert-ToOracleDateExpression $Scenario.EndTime
    $versionDateExpr = if ($null -ne $Scenario.VersionDate) {
        Convert-ToOracleDateExpression $Scenario.VersionDate
    } else {
        "null"
    }
    $maxVersionLiteral = if ($null -ne $Scenario.VersionDate) { "'F'" } else { "'T'" }
    $retrieveFunction = if ($Scenario.IncludeEntryDate) {
        "cwms_20.cwms_ts.retrieve_ts_entry_out_tab"
    } else {
        "cwms_20.cwms_ts.retrieve_ts_out_tab"
    }

    $rowProjection = if ($Scenario.IncludeEntryDate) {
        @"
json_object(
  'date_time' value round((date_time - date '1970-01-01') * 86400000),
  'value' value value,
  'quality_code' value quality_code,
  'data_entry_date' value case
    when data_entry_date is null then null
    else round((cast(data_entry_date as date) - date '1970-01-01') * 86400000)
  end null on null
)
"@
    } else {
        @"
json_object(
  'date_time' value round((date_time - date '1970-01-01') * 86400000),
  'value' value value,
  'quality_code' value quality_code
)
"@
    }

    return @"
set heading off feedback off verify off pagesize 0 linesize 32767 long 1000000 longchunksize 1000000 trimspool on
with oracle_rows as (
  select *
    from table($retrieveFunction(
      $seriesLiteral,
      $unitsLiteral,
      $beginExpr,
      $endExpr,
      'UTC',
      'T',
      'T',
      'T',
      'F',
      'F',
      $versionDateExpr,
      $maxVersionLiteral,
      $officeLiteral
    ))
)
select json_object(
  'row_count' value (select count(*) from oracle_rows),
  'rows' value nvl(
    (
      select json_arrayagg(
        $rowProjection
        returning clob
      )
      from (
        select *
          from oracle_rows
         order by date_time
      )
    ),
    '[]'
  ) format json
  returning clob
)
from dual;
exit;
"@
}

function Get-OracleScenarioResult {
    param($Scenario)

    $responseFile = Join-Path $ResponsesDir ("parity-{0}-oracle.json" -f $Scenario.Name)
    $raw = Invoke-OracleSql -Sql (Get-OracleRowsSql -Scenario $Scenario) -Label ("oracle-{0}" -f $Scenario.Name)
    $json = (($raw -split "\r?\n") | ForEach-Object { $_.Trim() } | Where-Object { $_ }) -join ""
    Set-Content -LiteralPath $responseFile -Value $json -Encoding ASCII
    $payload = $json | ConvertFrom-Json
    $rows = @()
    foreach ($entry in $payload.rows) {
        $row = [ordered]@{
            date_time = [long]$entry.date_time
            value = if ($null -eq $entry.value) { $null } else { [double]$entry.value }
            quality_code = [int]$entry.quality_code
        }
        if ($Scenario.IncludeEntryDate) {
            $row.data_entry_date = if ($null -ne $entry.PSObject.Properties["data_entry_date"] -and $null -ne $entry.data_entry_date) {
                [long]$entry.data_entry_date
            } else {
                $null
            }
        }
        $rows += [pscustomobject]$row
    }

    if (-not $KeepResponses -and (Test-Path -LiteralPath $responseFile)) {
        Remove-Item -LiteralPath $responseFile -Force
        $responseFile = $null
    }

    return [pscustomobject]@{
        Payload = $payload
        Rows = @($rows | Sort-Object date_time)
        ResponseFile = $responseFile
    }
}

function Test-RowEquality {
    param(
        $Expected,
        $Actual,
        [bool]$IncludeEntryDate
    )

    if ($Expected.date_time -ne $Actual.date_time) {
        return $false
    }

    if ($Expected.quality_code -ne $Actual.quality_code) {
        return $false
    }

    if ($null -eq $Expected.value -and $null -ne $Actual.value) {
        return $false
    }

    if ($null -ne $Expected.value -and $null -eq $Actual.value) {
        return $false
    }

    if ($null -ne $Expected.value -and $null -ne $Actual.value) {
        if ([math]::Abs([double]$Expected.value - [double]$Actual.value) -gt $FloatTolerance) {
            return $false
        }
    }

    if ($IncludeEntryDate) {
        if ($Expected.data_entry_date -ne $Actual.data_entry_date) {
            return $false
        }
    }

    return $true
}

function Compare-ScenarioRows {
    param(
        [object[]]$ExpectedRows,
        [object[]]$ActualRows,
        [bool]$IncludeEntryDate
    )

    $mismatchCount = 0
    $firstMismatch = $null
    $maxLength = [math]::Max($ExpectedRows.Count, $ActualRows.Count)

    for ($index = 0; $index -lt $maxLength; $index++) {
        $expected = if ($index -lt $ExpectedRows.Count) { $ExpectedRows[$index] } else { $null }
        $actual = if ($index -lt $ActualRows.Count) { $ActualRows[$index] } else { $null }

        $equal = $false
        if ($null -ne $expected -and $null -ne $actual) {
            $equal = Test-RowEquality -Expected $expected -Actual $actual -IncludeEntryDate $IncludeEntryDate
        }

        if (-not $equal) {
            $mismatchCount++
            if ($null -eq $firstMismatch) {
                $firstMismatch = [pscustomobject]@{
                    index = $index
                    expected = $expected
                    actual = $actual
                }
            }
        }
    }

    return [pscustomobject]@{
        mismatch_count = $mismatchCount
        first_mismatch = $firstMismatch
    }
}

function Test-MetadataExpectation {
    param(
        $Scenario,
        $CdaResult,
        $OracleResult
    )

    $metadataMismatches = @()
    if ($CdaResult.Payload.total -ne $OracleResult.Payload.row_count) {
        $metadataMismatches += [pscustomobject]@{
            field = "total"
            expected = [int]$OracleResult.Payload.row_count
            actual = $CdaResult.Payload.total
        }
    }

    if ($CdaResult.Payload.'date-version-type' -ne $Scenario.ExpectedDateVersionType) {
        $metadataMismatches += [pscustomobject]@{
            field = "date-version-type"
            expected = $Scenario.ExpectedDateVersionType
            actual = $CdaResult.Payload.'date-version-type'
        }
    }

    if ($CdaResult.Payload.'interval-offset' -ne $Scenario.ExpectedIntervalOffset) {
        $metadataMismatches += [pscustomobject]@{
            field = "interval-offset"
            expected = $Scenario.ExpectedIntervalOffset
            actual = $CdaResult.Payload.'interval-offset'
        }
    }

    if ($CdaResult.Payload.interval -ne $Scenario.ExpectedInterval) {
        $metadataMismatches += [pscustomobject]@{
            field = "interval"
            expected = $Scenario.ExpectedInterval
            actual = $CdaResult.Payload.interval
        }
    }

    if ($null -ne $Scenario.VersionDate) {
        $expectedVersionDate = $Scenario.VersionDate.ToString("yyyy-MM-ddTHH:mm:ssZ")
        if ($CdaResult.Payload.'version-date' -ne $expectedVersionDate) {
            $metadataMismatches += [pscustomobject]@{
                field = "version-date"
                expected = $expectedVersionDate
                actual = $CdaResult.Payload.'version-date'
            }
        }
    }

    return @($metadataMismatches)
}

New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null
New-Item -ItemType Directory -Path $ResponsesDir -Force | Out-Null

$scenarioMap = @{}
foreach ($scenario in Get-ScenarioDefinitions) {
    $scenarioMap[$scenario.Name] = $scenario
}

$requestedScenarios = foreach ($scenarioName in $Scenarios) {
    if (-not $scenarioMap.ContainsKey($scenarioName)) {
        throw "Unknown scenario '$scenarioName'. Available scenarios: $($scenarioMap.Keys -join ', ')"
    }
    $scenarioMap[$scenarioName]
}

Wait-ForCdaReady -Url ("{0}/offices/{1}" -f $CdaBaseUrl.TrimEnd("/"), [uri]::EscapeDataString($Office))

$results = @()
$failedScenarios = @()
foreach ($scenario in $requestedScenarios) {
    Invoke-OracleSql -Sql (Get-SeedSql -Scenario $scenario) -Label ("seed-{0}" -f $scenario.Name) | Out-Null

    $oracleResult = Get-OracleScenarioResult -Scenario $scenario
    $cdaResult = Get-CdaScenarioResult -Scenario $scenario
    $rowComparison = Compare-ScenarioRows -ExpectedRows $oracleResult.Rows -ActualRows $cdaResult.Rows -IncludeEntryDate $scenario.IncludeEntryDate
    $metadataMismatches = @(Test-MetadataExpectation -Scenario $scenario -CdaResult $cdaResult -OracleResult $oracleResult)
    $passed = $cdaResult.HttpCode -eq 200 -and $rowComparison.mismatch_count -eq 0 -and $metadataMismatches.Count -eq 0

    $result = [pscustomobject]@{
        scenario = $scenario.Name
        http_code = $cdaResult.HttpCode
        time_total_seconds = $cdaResult.TimeTotalSeconds
        request_url = $cdaResult.RequestUrl
        include_entry_date = [bool]$scenario.IncludeEntryDate
        version_date = if ($null -ne $scenario.VersionDate) { $scenario.VersionDate.ToString("o") } else { $null }
        expected_row_count = [int]$oracleResult.Payload.row_count
        actual_row_count = $cdaResult.Rows.Count
        reported_total = $cdaResult.Payload.total
        expected_date_version_type = $scenario.ExpectedDateVersionType
        actual_date_version_type = $cdaResult.Payload.'date-version-type'
        expected_interval = $scenario.ExpectedInterval
        actual_interval = $cdaResult.Payload.interval
        expected_interval_offset = $scenario.ExpectedIntervalOffset
        actual_interval_offset = $cdaResult.Payload.'interval-offset'
        metadata_mismatches = $metadataMismatches
        row_mismatch_count = $rowComparison.mismatch_count
        first_row_mismatch = $rowComparison.first_mismatch
        oracle_response_file = $oracleResult.ResponseFile
        cda_response_file = $cdaResult.ResponseFile
        passed = $passed
    }

    $results += $result
    if (-not $passed) {
        $failedScenarios += $result
    }
}

$gitBranch = (& git branch --show-current 2>$null)
$gitBranchExitCode = $LASTEXITCODE
$gitCommit = (& git rev-parse HEAD 2>$null)
$gitCommitExitCode = $LASTEXITCODE
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$resultFile = Join-Path $ResultsDir ("timeseries-parity-{0}.json" -f $timestamp)
$summary = [pscustomobject]@{
    total_scenarios = $results.Count
    passed_scenarios = @($results | Where-Object { $_.passed }).Count
    failed_scenarios = @($results | Where-Object { -not $_.passed }).Count
}

$payload = [pscustomobject]@{
    parity = "timeseries"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    git_branch = if ($gitBranchExitCode -eq 0 -and $null -ne $gitBranch) { $gitBranch.Trim() } else { $null }
    git_commit = if ($gitCommitExitCode -eq 0 -and $null -ne $gitCommit) { $gitCommit.Trim() } else { $null }
    office = $Office
    summary = $summary
    results = $results
}

$payload | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resultFile -Encoding ASCII
$payload | ConvertTo-Json -Depth 8

if ($failedScenarios.Count -gt 0) {
    $failedNames = ($failedScenarios | ForEach-Object { $_.scenario }) -join ", "
    throw "Parity check found mismatches in: $failedNames. Results saved to $resultFile"
}
