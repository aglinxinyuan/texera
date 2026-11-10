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
    [ValidateRange(1, 100)][int]$Repetitions = 5,
    [ValidateRange(0, 20)][int]$Warmups = 2,
    [string]$JavaHome = 'C:/Users/linxi/.jdks/jbr-17.0.14'
)

$ErrorActionPreference = 'Stop'
$taskPath = $env:PATH
Push-Location (Resolve-Path (Join-Path $PSScriptRoot '../..'))
try {
    if (Test-Path -LiteralPath 'C:/Program Files/Git/bin/bash.exe') {
        $env:PATH = 'C:/Program Files/Git/bin;' + $env:PATH
    }
    New-Item -ItemType Directory -Force -Path 'target/xy-planning' | Out-Null
    $revision = (git rev-parse HEAD).Trim()
    $sourcePaths = @(
        'amber/src/main/scala/org/apache/texera/amber/engine/architecture/scheduling/CostBasedScheduleGenerator.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningComparison.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningComparisonSpec.scala'
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
        java_home = $JavaHome
        os = [System.Environment]::OSVersion.ToString()
        cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)
    } | ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 'target/xy-planning/metadata.json'

    Write-Host "X/Y pilot on $revision; output: target/xy-planning/"
    & sbt -java-home $JavaHome `
        'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.XYPlanningComparisonSpec' `
        "WorkflowExecutionService/Test/runMain org.apache.texera.amber.engine.architecture.scheduling.XYPlanningComparison target/xy-planning/results.csv $Repetitions $Warmups" `
        *> 'target/xy-planning/run.log'
    if ($LASTEXITCODE -ne 0) {
        Get-Content 'target/xy-planning/run.log' -Tail 35
        throw 'X/Y pilot failed; see target/xy-planning/run.log'
    }
    if ((git rev-parse HEAD).Trim() -ne $revision) { throw 'Checkout changed during the run' }
    foreach ($sourcePath in $sourcePaths) {
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath $sourcePath).Hash -ne $hashes[$sourcePath]) {
            throw "Source changed during the run: $sourcePath"
        }
    }
    Get-Content 'target/xy-planning/run.log' -Tail 12
} finally {
    Pop-Location
    $env:PATH = $taskPath
}
