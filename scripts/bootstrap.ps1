<#
===========================================================================
 Levanta el sistema COMPLETO desde cero con un solo comando, en Windows.

   .\scripts\bootstrap.ps1
   .\scripts\bootstrap.ps1 -SampleSize 1000000
   .\scripts\bootstrap.ps1 -SkipIngest
   .\scripts\bootstrap.ps1 -Benchmark
   .\scripts\bootstrap.ps1 -Fresh          # borra volumenes y empieza limpio

 Equivalente a scripts/bootstrap.sh. Se incluye porque el enunciado exige que
 el sistema se pueda levantar "en otro equipo sin pasos manuales ocultos", y
 Git Bash no siempre esta disponible en una maquina con Windows.
===========================================================================
#>
[CmdletBinding()]
param(
    [int]    $SampleSize = 2000000,
    [switch] $SkipIngest,
    [switch] $SkipSpark,
    [switch] $Benchmark,
    [switch] $NoJenkins,
    [switch] $Fresh
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "  OK   $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "  !!   $msg" -ForegroundColor Yellow }
function Fail($msg) { Write-Host "  XX   $msg" -ForegroundColor Red }
function Die($msg)  { Fail $msg; exit 1 }

# =========================================================================
Step '1/8  Comprobando requisitos'
# =========================================================================
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Die 'Docker no esta instalado o no esta en el PATH.'
}
docker compose version *> $null
if (-not $?) { Die "Falta el plugin 'docker compose' (v2)." }
docker info *> $null
if (-not $?) { Die 'El demonio de Docker no responde. Arranque Docker Desktop.' }

$serverVersion = docker version --format '{{.Server.Version}}'
$composeVersion = docker compose version --short
Ok "Docker $serverVersion y Compose $composeVersion"

$memBytes = [int64](docker info --format '{{.MemTotal}}')
$memGb = [math]::Round($memBytes / 1GB, 1)
if ($memGb -lt 7) {
    Warn "Docker solo tiene $memGb GB de RAM. Se recomiendan 8 GB o mas."
    Warn "Cree $env:USERPROFILE\.wslconfig con:   [wsl2]`n                                     memory=8GB"
    Warn 'Despues:  wsl --shutdown  y reinicie Docker Desktop.'
} else {
    Ok "Memoria disponible para Docker: $memGb GB"
}

# =========================================================================
Step '2/8  Preparando la configuracion (.env)'
# =========================================================================
if (-not (Test-Path '.env')) {
    Copy-Item '.env.example' '.env'
    # Contrasena aleatoria: ningun secreto por defecto queda en el repositorio
    $bytes = New-Object 'byte[]' 24
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $generated = ([Convert]::ToBase64String($bytes) -replace '[/+=]', '').Substring(0, 20)
    (Get-Content '.env' -Raw).Replace('cambiame_en_local', $generated) |
        Set-Content '.env' -NoNewline -Encoding utf8
    Ok '.env creado con una contrasena de MongoDB aleatoria'
} else {
    Ok '.env ya existe (no se sobrescribe)'
}

$envText = Get-Content '.env' -Raw
if ($envText -match '(?m)^KAGGLE_USERNAME=\S+') {
    Ok 'Credenciales de Kaggle configuradas en .env'
} elseif ((Test-Path 'secrets\kaggle.json') -or
          (Test-Path (Join-Path $env:USERPROFILE '.kaggle\kaggle.json'))) {
    Ok 'kaggle.json encontrado'
} else {
    Warn 'Sin credenciales de Kaggle: la ingesta generara datos SINTETICOS.'
    Warn 'Para el dataset real coloque kaggle.json en .\secrets\ (ver README).'
}

if ($Fresh) {
    Step '2b/8  -Fresh: eliminando contenedores y volumenes previos'
    docker compose down -v --remove-orphans
    Ok 'Estado anterior eliminado'
}

# =========================================================================
Step '3/8  Construyendo las imagenes'
# =========================================================================
docker compose build mongo dask-scheduler spark-master api tests benchmark
if (-not $?) { Die 'Fallo la construccion de las imagenes.' }
Ok 'Imagenes construidas'
docker images --filter 'reference=geobigdata/*' --format '       {{.Repository}}:{{.Tag}}  {{.Size}}'

# =========================================================================
Step '4/8  Levantando la infraestructura'
# =========================================================================
$services = @('mongo', 'dask-scheduler', 'dask-worker', 'spark-master',
              'spark-worker', 'api')
if (-not $NoJenkins) { $services += 'jenkins' }

docker compose up -d --remove-orphans @services
if (-not $?) { Die 'Fallo el arranque de los servicios.' }
Ok 'Servicios iniciados'

function Wait-Healthy {
    param([string]$Service, [int]$Attempts = 60)
    Write-Host ('       esperando a {0,-16}' -f $Service) -NoNewline
    for ($i = 0; $i -lt $Attempts; $i++) {
        $cid = (docker compose ps -q $Service | Select-Object -First 1)
        if ($cid) {
            $state = docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $cid
            if ($state -in @('healthy', 'running')) {
                Write-Host " [$state]" -ForegroundColor Green
                return $true
            }
        }
        Write-Host '.' -NoNewline
        Start-Sleep -Seconds 3
    }
    Write-Host ' TIMEOUT' -ForegroundColor Red
    docker compose logs --tail 60 $Service
    return $false
}

if (-not (Wait-Healthy 'mongo' 60))          { Die 'MongoDB no llego a estar sano' }
if (-not (Wait-Healthy 'dask-scheduler' 40)) { Die 'El scheduler de Dask no arranco' }
if (-not (Wait-Healthy 'spark-master' 40))   { Die 'El master de Spark no arranco' }
if (-not (Wait-Healthy 'api' 60))            { Die 'La API no arranco' }
Ok 'Todos los servicios responden'

# =========================================================================
if (-not $SkipIngest) {
    Step "5/8  Ingesta con Dask  (objetivo: $SampleSize registros)"
    Warn 'Con el dataset completo de Kaggle la descarga son ~1,2 GB: puede tardar.'
    docker compose run --rm -e "SAMPLE_SIZE=$SampleSize" ingestion
    if (-not $?) { Die 'Fallo la ingesta.' }
    Ok 'Datos cargados en MongoDB con indice 2dsphere'
} else {
    Step '5/8  Ingesta omitida (-SkipIngest)'
}

# =========================================================================
if (-not $SkipSpark) {
    Step '6/8  Agregaciones espaciales y temporales con Spark'
    docker compose run --rm spark-job
    if (-not $?) { Die 'Fallo el procesamiento con Spark.' }
    Ok 'Colecciones agregadas creadas'
} else {
    Step '6/8  Procesamiento con Spark omitido (-SkipSpark)'
}

# =========================================================================
if ($Benchmark) {
    Step '7/8  Benchmark Dask vs Spark'
    docker compose run --rm benchmark
    Ok 'Benchmark terminado (resultados en el volumen /data/benchmark)'
} else {
    Step '7/8  Benchmark omitido (use -Benchmark para ejecutarlo)'
}

# =========================================================================
Step '8/8  Verificacion final'
# =========================================================================
$apiPort = 5000
if ($envText -match '(?m)^API_PORT=(\d+)') { $apiPort = [int]$Matches[1] }
$base = "http://localhost:$apiPort"

function Test-Endpoint {
    param([string]$Description, [string]$Url)
    try {
        $res = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 30
        if ($res.StatusCode -eq 200) { Ok $Description; return $true }
        Fail "$Description (HTTP $($res.StatusCode))"
    } catch {
        Fail "$Description ($($_.Exception.Message))"
    }
    return $false
}

Test-Endpoint 'salud de la API'          "$base/api/v1/health"          | Out-Null
Test-Endpoint 'estadisticas'             "$base/api/v1/stats"           | Out-Null
Test-Endpoint 'consulta por radio'       "$base/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=10000&limit=3" | Out-Null
Test-Endpoint 'consulta en poligono'     "$base/api/v1/within?min_lon=-118.5&min_lat=33.9&max_lon=-118.1&max_lat=34.1&limit=3" | Out-Null
Test-Endpoint 'agregacion por cercania'  "$base/api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000" | Out-Null
Test-Endpoint 'resultados de Spark'      "$base/api/v1/aggregations"    | Out-Null

Write-Host "`n--- Estado de la base de datos ---"
try {
    (Invoke-WebRequest -Uri "$base/api/v1/stats" -UseBasicParsing).Content
} catch { Warn 'No se pudo consultar /stats' }

Write-Host ''
Write-Host '======================================================================' -ForegroundColor Green
Write-Host '  SISTEMA EN MARCHA' -ForegroundColor Green
Write-Host '======================================================================' -ForegroundColor Green
Write-Host "  Mapa interactivo   $base/"
Write-Host "  Documentacion API  $base/api/v1/docs"
Write-Host '  Panel de Dask      http://localhost:8787'
Write-Host '  Interfaz de Spark  http://localhost:8080'
if (-not $NoJenkins) {
    Write-Host '  Jenkins            http://localhost:8088'
    Write-Host '                     contrasena inicial:'
    Write-Host '                     docker compose exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword'
}
Write-Host ''
Write-Host '  Parar todo             docker compose down'
Write-Host '  Parar y borrar datos   docker compose down -v'
Write-Host '======================================================================' -ForegroundColor Green
Write-Host ''
