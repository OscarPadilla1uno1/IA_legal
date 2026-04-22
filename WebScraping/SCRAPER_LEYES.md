# Scraper de Leyes de Honduras

Este scraper descarga PDFs de leyes hondurenas a disco local, con salida por defecto en:

`D:\LeyesHonduras`

## Fuentes soportadas

- `cedij`: Biblioteca de Legislacion Hondurena del Poder Judicial
- `tsc`: Biblioteca Virtual del Tribunal Superior de Cuentas
- `all`: intenta ambas

## Archivos de salida

- PDFs descargados
- `manifest_leyes.jsonl`: una linea JSON por documento descargado
- `estado_scraper_leyes.json`: ultimo avance por fuente

## Uso rapido

Desde `C:\Project\WebScraping`:

```powershell
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$env:PYTHONPATH = 'C:\Project\WebScraping'

& $py .\scrape_leyes_honduras.py --source all
```

## Prueba corta

```powershell
& $py .\scrape_leyes_honduras.py --source cedij --max-pages 2 --headful
```

## Notas importantes

- `CEDIJ` indica en su portal que su biblioteca publica incluye documentos desde 2009 en adelante.
- `TSC` sirve como fuente complementaria y de respaldo.
- El scraper deduplica por URL y por ruta local usando el manifiesto.
- Si `D:` no esta disponible, cae automaticamente a `C:\Project\Output\LeyesHonduras`.
