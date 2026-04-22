# Recuperacion de la Base y Dashboard

Este documento deja los pasos para volver a montar el entorno exactamente como quedo funcional:

- Base buena: volumen Docker `db_legal_ia_data`
- Contenedor principal PostgreSQL: `legal_ia_db`
- `pgAdmin`: `legal_ia_gui`
- Dashboard de pruebas RAG: `dashboard_busqueda.py`

## Estado correcto final

La base buena fue la del volumen:

```text
db_legal_ia_data
```

Esa base quedo validada con estos conteos:

```text
sentencias: 17167
fragmentos: 296609
vectorizados: 296609
pendientes: 0
```

La copia incorrecta o incompleta fue:

```text
docker_legal_ia_data
```

Esa copia ya se elimino para evitar confusion.

## 1. Levantar la base buena

Abrir PowerShell en `C:\Project`.

### Ver si ya esta corriendo

```powershell
docker ps -a --filter "name=legal_ia_db"
```

### Si no esta corriendo, levantarla

```powershell
docker run -d `
  --name legal_ia_db `
  --network docker_default `
  --network-alias db `
  --network-alias legal_ia_db `
  -p 5432:5432 `
  -e POSTGRES_DB=legal_ia `
  -e POSTGRES_USER=root `
  -e POSTGRES_PASSWORD=rootpassword `
  -v db_legal_ia_data:/var/lib/postgresql/data `
  --restart always `
  ankane/pgvector:latest
```

### Si el contenedor ya existe pero esta detenido

```powershell
docker start legal_ia_db
```

## 2. Verificar la base

Abrir PowerShell en `C:\Project\DB`.

```powershell
$env:PYTHONPATH = 'C:\Project\DB\venv_bge\Lib\site-packages'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'

@'
import psycopg2
conn = psycopg2.connect(dbname='legal_ia', user='root', password='rootpassword', host='localhost', port='5432')
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM sentencias")
print("sentencias =", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM fragmentos_texto")
print("fragmentos =", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NOT NULL")
print("vectorizados =", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NULL")
print("pendientes =", cur.fetchone()[0])
cur.close()
conn.close()
'@ | & $py -
```

El resultado correcto esperado es:

```text
sentencias = 17167
fragmentos = 296609
vectorizados = 296609
pendientes = 0
```

## 3. Levantar pgAdmin

### Ver si ya existe

```powershell
docker ps -a --filter "name=legal_ia_gui"
```

### Si no existe, crearlo

```powershell
docker run -d `
  --name legal_ia_gui `
  --network docker_default `
  -p 8080:80 `
  -e PGADMIN_DEFAULT_EMAIL=admin@admin.com `
  -e PGADMIN_DEFAULT_PASSWORD=root `
  --restart always `
  dpage/pgadmin4
```

### Si existe pero esta detenido

```powershell
docker start legal_ia_gui
```

### Acceso

Abrir:

```text
http://127.0.0.1:8080
```

Credenciales:

```text
correo: admin@admin.com
password: root
```

### Servidor dentro de pgAdmin

Si hay que registrar la conexion manualmente:

```text
Name: legal_ia
Host: db
Port: 5432
Username: root
Password: rootpassword
```

## 4. Levantar el dashboard de pruebas RAG

Abrir PowerShell en `C:\Project\DB`.

### Forma simple

```powershell
.\launch_dashboard.ps1
```

O con doble clic:

```text
launch_dashboard.bat
```

### Forma explicita

```powershell
$env:PYTHONPATH = 'C:\Project\DB;C:\Project\DB\venv_bge\Lib\site-packages'
$env:SEARCH_DEVICE = 'cpu'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'

& $py .\dashboard_busqueda.py --host 127.0.0.1 --port 8765
```

Abrir:

```text
http://127.0.0.1:8765
```

## 5. Scripts utiles para probar la base

Siempre desde `C:\Project\DB`:

```powershell
$env:PYTHONPATH = 'C:\Project\DB\venv_bge\Lib\site-packages'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
```

### Diagnostico

```powershell
& $py .\db_diagnostico.py
```

### Busqueda semantica simple

```powershell
& $py .\buscador_rag.py --query "es inadmisible el recurso de amparo cuando se alegan cuestiones de mera legalidad" --top 5
```

### Busqueda hibrida con re-ranking

```powershell
& $py .\test_retrieval.py
```

### Asistente con LLM

```powershell
& $py .\asistente_legal.py
```

## 6. Comandos de recuperacion si vuelve a aparecer otra copia

### Ver contenedores

```powershell
docker ps -a
```

### Ver volumenes

```powershell
docker volume ls
```

### Inspeccionar que volumen usa el contenedor activo

```powershell
docker inspect legal_ia_db
```

En el resultado, el correcto debe apuntar a:

```text
db_legal_ia_data
```

## 7. Comandos para dejar exactamente el estado correcto

Si alguna vez quieres rehacer la sustitucion completa:

### Detener y borrar la copia activa equivocada

```powershell
docker rm -f legal_ia_db
```

### Borrar el volumen incorrecto del 20 de abril

```powershell
docker volume rm docker_legal_ia_data
```

### Levantar la copia buena como principal

```powershell
docker run -d `
  --name legal_ia_db `
  --network docker_default `
  --network-alias db `
  --network-alias legal_ia_db `
  -p 5432:5432 `
  -e POSTGRES_DB=legal_ia `
  -e POSTGRES_USER=root `
  -e POSTGRES_PASSWORD=rootpassword `
  -v db_legal_ia_data:/var/lib/postgresql/data `
  --restart always `
  ankane/pgvector:latest
```

## 8. Advertencias

### No correr ahora

No ejecutar este script sobre la base buena:

```text
vectorizador_bge_nativo.py
```

Motivo:

- si detecta `0` pendientes, puede resetear embeddings para reprocesar
- esa base ya esta completa y no necesita revectorizacion

### GPU

Para el dashboard y pruebas dejamos `cpu` como modo estable porque la carga con `cuda` fue inconsistente en esta maquina.

## 9. Archivos relevantes

- `C:\Project\DB\dashboard_busqueda.py`
- `C:\Project\DB\dashboard_busqueda.html`
- `C:\Project\DB\launch_dashboard.ps1`
- `C:\Project\DB\launch_dashboard.bat`
- `C:\Project\DB\buscador_rag.py`
- `C:\Project\DB\test_retrieval.py`
- `C:\Project\DB\asistente_legal.py`

## 10. Resumen corto

Si solo quieres volver a dejar todo arriba:

1. Levanta la base:

```powershell
docker start legal_ia_db
```

2. Levanta pgAdmin:

```powershell
docker start legal_ia_gui
```

3. Levanta el dashboard:

```powershell
Set-Location C:\Project\DB
.\launch_dashboard.ps1
```

4. Abre:

```text
pgAdmin: http://127.0.0.1:8080
dashboard: http://127.0.0.1:8765
```
