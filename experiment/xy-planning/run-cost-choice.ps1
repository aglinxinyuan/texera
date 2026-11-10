<#
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
#>
param(
    [string]$JavaHome = 'C:/Users/linxi/.jdks/jbr-17.0.14',
    [string]$Python = 'C:/Users/linxi/AppData/Local/Programs/Python/Python312/python.exe'
)
$ErrorActionPreference = 'Stop'
$savedPath = $env:PATH
Push-Location (Resolve-Path (Join-Path $PSScriptRoot '../..'))
try {
    $env:PATH = 'C:/Program Files/Git/bin;' + $env:PATH
    Get-Command sbt -ErrorAction Stop | Out-Null
    $output = 'target/xy-planning-cost-choice/' + (Get-Date -Format 'yyyyMMdd-HHmmss')
    New-Item -ItemType Directory -Path $output | Out-Null
    $revision = (git rev-parse HEAD).Trim()
    $sources = @(git ls-files 'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/*XY*.scala' 'amber/src/main/scala/org/apache/texera/amber/engine/architecture/scheduling/CostBasedScheduleGenerator.scala' 'experiment/xy-planning/*')
    $hashes = @{}
    foreach ($file in $sources) { $hashes[$file] = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash }
    @{
        revision=$revision; source_sha256=$hashes; repetitions=3; warmups=0; forks=1;
        pruning=$true; java_home=$JavaHome; purpose='correctness only; no timing claim';
        utc=(Get-Date).ToUniversalTime().ToString('o')
    } | ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 "$output/metadata.json"
    Write-Host "Native cost-choice check on $revision; output: $output"
    # PowerShell 5 wraps known native stderr warnings as ErrorRecord; check exit codes explicitly.
    $ErrorActionPreference = 'Continue'
    & sbt -java-home $JavaHome 'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.NativeCostChoiceXYSpec' "WorkflowExecutionService/Test/runMain org.apache.texera.amber.engine.architecture.scheduling.NativeCostChoiceXYSpec $output/fork-0 3 0 0" *> "$output/run.log"
    $code = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($code -ne 0) { throw "Native compilation/check failed ($code); see $output/run.log" }
    if ((git rev-parse HEAD).Trim() -ne $revision) { throw 'HEAD changed during check' }
    foreach ($file in $sources) {
        if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash -ne $hashes[$file]) { throw "Source changed: $file" }
    }
    & $Python -I experiment/xy-planning/check-cost-choice.py $output
    if ($LASTEXITCODE -ne 0) { throw 'Independent enumeration rejected the exported result' }
    Write-Host "Verified $output"
} finally {
    $env:PATH = $savedPath
    Pop-Location
}
