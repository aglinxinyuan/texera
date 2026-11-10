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
    [ValidateRange(1, 100)][int]$Repetitions = 3,
    [ValidateRange(0, 20)][int]$Warmups = 1,
    [ValidateRange(1, 10)][int]$Forks = 3,
    [string]$JavaHome = 'C:/Users/linxi/.jdks/jbr-17.0.14',
    [string]$Python = 'C:/Users/linxi/AppData/Local/Programs/Python/Python312/python.exe'
)

$ErrorActionPreference = 'Stop'
$taskPath = $env:PATH
Push-Location (Resolve-Path (Join-Path $PSScriptRoot '../..'))
try {
    if (Test-Path -LiteralPath 'C:/Program Files/Git/bin/bash.exe') {
        $env:PATH = 'C:/Program Files/Git/bin;' + $env:PATH
    }
    $outputRoot = 'target/xy-planning-extended/' + (Get-Date -Format 'yyyyMMdd-HHmmss')
    New-Item -ItemType Directory -Path $outputRoot | Out-Null
    $revision = (git rev-parse HEAD).Trim()
    $sourcePaths = @(
        'amber/src/main/scala/org/apache/texera/amber/engine/architecture/scheduling/CostBasedScheduleGenerator.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningComparison.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningComparisonSpec.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningSweep.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningSweepSpec.scala',
        'experiment/xy-planning/run-extended.ps1',
        'experiment/xy-planning/analyze.py',
        'experiment/xy-planning/oracle.py'
    )
    $hashes = @{}
    foreach ($sourcePath in $sourcePaths) {
        $hashes[$sourcePath] = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourcePath).Hash
    }
    @{
        revision = $revision
        source_sha256 = $hashes
        utc = (Get-Date).ToUniversalTime().ToString('o')
        repetitions = $Repetitions
        warmups = $Warmups
        forks = $Forks
        java_home = $JavaHome
        os = [System.Environment]::OSVersion.ToString()
        cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)
    } | ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 "$outputRoot/metadata.json"
    Write-Host "X/Y sweep on $revision; output: $outputRoot"
    & sbt -java-home $JavaHome `
        'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.XYPlanningSweepSpec org.apache.texera.amber.engine.architecture.scheduling.XYPlanningComparisonSpec org.apache.texera.amber.engine.architecture.scheduling.CostBasedScheduleGeneratorSpec' `
        *> "$outputRoot/tests.log"
    if ($LASTEXITCODE -ne 0) { throw "Tests failed: $outputRoot/tests.log" }
    for ($fork = 0; $fork -lt $Forks; $fork++) {
        $forkRoot = "$outputRoot/fork-$fork"
        New-Item -ItemType Directory -Path $forkRoot | Out-Null
        Write-Host "Starting fresh JVM $fork of $Forks"
        & sbt -java-home $JavaHome `
            "WorkflowExecutionService/Test/runMain org.apache.texera.amber.engine.architecture.scheduling.XYPlanningSweep $forkRoot/results.csv $Repetitions $Warmups $fork" `
            *> "$forkRoot/run.log"
        if ($LASTEXITCODE -ne 0) {
            Get-Content "$forkRoot/run.log" -Tail 30
            throw "Sweep failed in fork $fork"
        }
        if ((git rev-parse HEAD).Trim() -ne $revision) { throw 'Checkout changed during run' }
        foreach ($sourcePath in $sourcePaths) {
            if ((Get-FileHash -Algorithm SHA256 -LiteralPath $sourcePath).Hash -ne $hashes[$sourcePath]) {
                throw "Source changed during run: $sourcePath"
            }
        }
        Get-Content "$forkRoot/run.log" -Tail 3
    }
    & $Python -X utf8 -I experiment/xy-planning/oracle.py $outputRoot
    if ($LASTEXITCODE -ne 0) { throw 'Independent minimum-cost verification failed' }
    & $Python -X utf8 -I experiment/xy-planning/analyze.py $outputRoot
    if ($LASTEXITCODE -ne 0) { throw 'Result verification/analysis failed' }
    Write-Host "Report: $outputRoot/REPORT.md"
} finally {
    Pop-Location
    $env:PATH = $taskPath
}
