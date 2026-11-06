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
    [ValidateRange(1,10000)][int]$Repetitions = 100,
    [ValidateRange(0,1000)][int]$Warmups = 20,
    [ValidateRange(1,10)][int]$Forks = 3,
    [string]$JavaHome = 'C:/Users/linxi/.jdks/jbr-17.0.14',
    [string]$Python = 'C:/Users/linxi/AppData/Local/Programs/Python/Python312/python.exe'
)
$ErrorActionPreference = 'Stop'
$taskPath = $env:PATH
Push-Location (Resolve-Path (Join-Path $PSScriptRoot '../..'))
try {
    $env:PATH = 'C:/Program Files/Git/bin;' + $env:PATH
    $output = 'target/xy-planning-compiled/' + (Get-Date -Format 'yyyyMMdd-HHmmss')
    New-Item -ItemType Directory -Path $output | Out-Null
    $revision = (git rev-parse HEAD).Trim()
    $sourcePaths = @(
        'amber/src/main/scala/org/apache/texera/amber/engine/architecture/scheduling/CostBasedScheduleGenerator.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/XYPlanningComparison.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/CompiledXYFixtures.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/CompiledXYPlanningSpec.scala',
        'amber/src/test/scala/org/apache/texera/amber/engine/architecture/scheduling/CompiledXYBenchmark.scala',
        'experiment/xy-planning/run-compiled.ps1',
        'experiment/xy-planning/analyze-compiled.py',
        'experiment/xy-planning/oracle.py'
    ) + @(Get-ChildItem -LiteralPath 'experiment/xy-planning/workflows' -File | ForEach-Object { 'experiment/xy-planning/workflows/' + $_.Name })
    $hashes = @{}
    foreach ($file in $sourcePaths) { $hashes[$file] = (Get-FileHash -Algorithm SHA256 -LiteralPath $file).Hash }
    @{
        revision=$revision; source_sha256=$hashes; utc=(Get-Date).ToUniversalTime().ToString('o')
        repetitions=$Repetitions; warmups=$Warmups; forks=$Forks; pruning=$true; java_home=$JavaHome
        os=[Environment]::OSVersion.ToString(); cpu=(Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)
    } | ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 "$output/metadata.json"
    Write-Host "Compiled comparison on $revision; output: $output"
    & sbt -java-home $JavaHome 'WorkflowExecutionService/testOnly org.apache.texera.amber.engine.architecture.scheduling.CompiledXYPlanningSpec org.apache.texera.amber.engine.architecture.scheduling.XYPlanningComparisonSpec org.apache.texera.amber.engine.architecture.scheduling.XYPlanningSweepSpec org.apache.texera.amber.engine.architecture.scheduling.CostBasedScheduleGeneratorSpec' *> "$output/tests.log"
    if ($LASTEXITCODE -ne 0) { Get-Content "$output/tests.log" -Tail 25; throw 'Tests failed' }
    for ($fork=0; $fork -lt $Forks; $fork++) {
        $folder="$output/fork-$fork"
        New-Item -ItemType Directory -Path $folder | Out-Null
        Write-Host "Starting fresh JVM $fork"
        & sbt -java-home $JavaHome "WorkflowExecutionService/Test/runMain org.apache.texera.amber.engine.architecture.scheduling.CompiledXYBenchmark $folder $Repetitions $Warmups $fork" *> "$folder/run.log"
        if ($LASTEXITCODE -ne 0) { Get-Content "$folder/run.log" -Tail 35; throw "JVM $fork failed" }
        if ((git rev-parse HEAD).Trim() -ne $revision) { throw 'Checkout changed during run' }
        foreach ($file in $sourcePaths) {
            if ((Get-FileHash -Algorithm SHA256 -LiteralPath $file).Hash -ne $hashes[$file]) { throw "Source changed: $file" }
        }
        Select-String -LiteralPath "$folder/run.log" -Pattern 'Compiled XY fork='
    }
    & $Python -X utf8 -I experiment/xy-planning/analyze-compiled.py $output
    if ($LASTEXITCODE -ne 0) { throw 'Independent verification failed' }
    Write-Host "Report: $output/REPORT.md"
} finally { Pop-Location; $env:PATH=$taskPath }
