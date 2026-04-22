param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8765,
    [ValidateSet("cpu", "cuda", "auto")]
    [string]$Device = "cpu"
)

$projectRoot = $PSScriptRoot
$python = "C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"

if (-not (Test-Path $python)) {
    throw "No se encontró Python en: $python"
}

$env:PYTHONPATH = "$projectRoot;$projectRoot\venv_bge\Lib\site-packages"
$env:SEARCH_DEVICE = $Device

Set-Location $projectRoot
& $python "$projectRoot\dashboard_busqueda.py" --host $HostName --port $Port
