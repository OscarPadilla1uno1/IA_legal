# Corpus Legal Canónico

Este módulo agrega un segundo corpus RAG al proyecto: leyes y códigos hondureños normalizados por `legislacion -> ley/codigo -> version -> articulo/nodo -> fragmento`.

## Que se agrego

- Nuevas tablas canonicas en `schema.sql`
- Migración ejecutable: `migrar_corpus_legal.py`
- Cargador de leyes desde JSON: `cargar_corpus_legal.py`
- Soporte rapido para `tipo_norma = ley | codigo`
- Jerarquia ligera para codigos en `nodos_normativos`
- Reconciliacion de referencias extraidas desde sentencias: `reconciliar_legislacion_canonica.py`
- Vectorizador para normas: `vectorizador_normativo_bge_nativo.py`
- Dashboard actualizado para consultar `jurisprudencia`, `leyes` o `ambos`
- Via lenta en paralelo:
  `schema_normativa_lenta.sql`, `migrar_normativa_lenta.py`, `cargar_normativa_lenta.py`, `vectorizador_normativa_lenta_bge_nativo.py`, `corpus_normativo_lento_ejemplo.json`, `VIA_LENTA_NORMATIVA.md`

## Via rapida

La documentacion de este archivo cubre la via rapida, que sirve para poblar rapido el corpus legal y volverlo consultable semanticamente sin una normalizacion profunda.

## Orden recomendado

1. Ejecutar la migración:

```powershell
$env:PYTHONPATH = 'C:\Project\DB\venv_bge\Lib\site-packages'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
& $py .\migrar_corpus_legal.py
```

2. Cargar el corpus canonico:

```powershell
& $py .\cargar_corpus_legal.py --input .\corpus_legal_ejemplo.json
```

3. Vectorizar normas:

```powershell
& $py .\vectorizador_normativo_bge_nativo.py
```

4. Reconciliar referencias historicas de sentencias:

```powershell
& $py .\reconciliar_legislacion_canonica.py
```

5. Levantar el dashboard:

```powershell
& $py .\dashboard_busqueda.py --host 127.0.0.1 --port 8765
```

## Formato JSON esperado

Usa `corpus_legal_ejemplo.json` como plantilla. La estructura soporta:

- `legislaciones[]`
- `legislaciones[].leyes[]`
- `leyes[].versiones[]`
- `versiones[].articulos[]`

Cada articulo se guarda como registro canonico y ademas se fragmenta para busqueda semantica.

Para codigos, el JSON tambien puede incluir:

- `leyes[].tipo_norma = "codigo"`
- `versiones[].nodos[]`

Los nodos permiten representar libros, titulos, capitulos y secciones sin salirnos de la via rapida.

## Via lenta

Si quieres el modelo mas completo y normalizado para todas las leyes, codigos, reformas y relaciones entre normas, usa:

- [schema_normativa_lenta.sql](C:\Project\DB\schema_normativa_lenta.sql)
- [migrar_normativa_lenta.py](C:\Project\DB\migrar_normativa_lenta.py)
- [cargar_normativa_lenta.py](C:\Project\DB\cargar_normativa_lenta.py)
- [vectorizador_normativa_lenta_bge_nativo.py](C:\Project\DB\vectorizador_normativa_lenta_bge_nativo.py)
- [corpus_normativo_lento_ejemplo.json](C:\Project\DB\corpus_normativo_lento_ejemplo.json)
- [VIA_LENTA_NORMATIVA.md](C:\Project\DB\VIA_LENTA_NORMATIVA.md)

La via lenta mantiene versionado formal, multiples fuentes, nodos jerarquicos genericos y relaciones entre documentos normativos. La busqueda actual sigue apoyandose en la via rapida; la lenta queda lista como siguiente nivel de canonizacion.

## Nota importante

El sistema ya quedo listo para indexar todas las leyes de Honduras, pero el contenido canonico completo todavia depende del dataset fuente que quieras cargar. Sin ese archivo, la parte jurisprudencial sigue funcionando y la parte normativa queda preparada para poblarse.
