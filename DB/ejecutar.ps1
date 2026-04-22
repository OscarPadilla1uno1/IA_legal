Set-Location C:\Project\DB
$env:PYTHONPATH = 'C:\Project\DB;C:\Project\DB\venv_bge\Lib\site-packages'
$env:SEARCH_DEVICE = 'cpu'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
& $py .\dashboard_busqueda.py --host 127.0.0.1 --port 8765
