# Submit SparkCompareJob to local Livy (docker compose must be up).
# Run from repo root after: mvn -q -DskipTests package
#   docker compose -f spark-cluster/docker-compose.yaml up -d

$ErrorActionPreference = "Stop"
$uri = "http://localhost:8998/batches"

$body = @{
  file       = "file:///data/cmdb-compare-service-0.0.1-SNAPSHOT-spark-job.jar"
  className  = "com.cmdb.compare.job.SparkCompareJob"
  args       = @(
    "file:///data/source.csv",
    "file:///data/target.csv",
    "id",
    "null",
    "null",
    "",
    "file:///data/out/",
    "http://127.0.0.1:9000",
    "dummy",
    "dummy"
  )
  conf       = @{
    "spark.master"                        = "local[*]"
    "spark.submit.deployMode"             = "client"
    "spark.yarn.submit.waitAppCompletion" = "false"
  }
} | ConvertTo-Json -Depth 6

Write-Host "POST $uri"
$response = Invoke-RestMethod -Uri $uri -Method Post -Body $body -ContentType "application/json; charset=utf-8"
Write-Host ($response | ConvertTo-Json -Depth 4)

if ($response.id) {
  $id = $response.id
  Write-Host "Polling batch $id ..."
  for ($i = 0; $i -lt 120; $i++) {
    Start-Sleep -Seconds 2
    $state = Invoke-RestMethod -Uri "http://localhost:8998/batches/$id/state" -Method Get
    Write-Host "state: $($state.state)"
    if ($state.state -eq "success" -or $state.state -eq "dead" -or $state.state -eq "error") { break }
  }
}
