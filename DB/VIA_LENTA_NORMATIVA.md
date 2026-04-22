# Via Lenta Normativa

La via lenta agrega un modelo mas canonico y completo para cargar el ordenamiento juridico hondureno sin depender de la estructura simplificada de la via rapida.

## Objetivo

Usala cuando necesites:

- versionado formal por documento
- relaciones entre normas (`reforma_a`, `deroga`, `fe_errata`, `interpreta`)
- estructura profunda (`libro -> titulo -> capitulo -> seccion -> articulo -> inciso`)
- multiples fuentes por version
- trazabilidad de PDFs locales y URLs oficiales

La via rapida sigue siendo util para arrancar rapido y para la busqueda actual. La via lenta vive en paralelo y puede convertirse despues en la fuente canonicamente dominante.

## Tablas nuevas

- `norma_tipos_documento`
- `norma_fuentes`
- `norma_documentos`
- `norma_aliases`
- `norma_versiones`
- `norma_version_fuentes`
- `norma_nodos`
- `norma_fragmentos`
- `norma_relaciones`
- `norma_mapeo_rapido`
- `norma_mapeo_legislacion_extraida`

## Flujo recomendado

1. Aplicar el esquema lento:

```powershell
$env:PYTHONPATH = 'C:\Project\DB\venv_bge\Lib\site-packages'
$py = 'C:\Users\Oscar Padilla\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
& $py .\migrar_normativa_lenta.py
```

2. Cargar un dataset lento:

```powershell
& $py .\cargar_normativa_lenta.py --input .\corpus_normativo_lento_ejemplo.json
```

3. Vectorizar la via lenta:

```powershell
& $py .\vectorizador_normativa_lenta_bge_nativo.py
```

La via lenta usa `norma_fragmentos` como corpus semantico separado.

## Formato JSON esperado

Usa `corpus_normativo_lento_ejemplo.json` como plantilla. Soporta:

- `fuentes[]`
- `documentos[]`
- `documentos[].versiones[]`
- `versiones[].fuentes[]`
- `versiones[].estructura[]`
- `documentos[].relaciones[]`

Cada nodo puede incluir:

- `ref`
- `tipo_nodo`
- `etiqueta`
- `identificador`
- `titulo`
- `articulo_numero`
- `texto`
- `metadata`
- `nodos[]`

## Diferencia clave frente a la via rapida

- Via rapida: `legislacion -> ley/codigo -> version -> articulo/nodo -> fragmento`
- Via lenta: `tipo -> documento -> version -> fuentes + nodos + relaciones -> fragmentos`

La via lenta no reemplaza automaticamente las tablas de la via rapida. La idea es permitir una migracion gradual:

1. cargar corpus canonico completo en via lenta
2. mapear via rapida hacia via lenta con `norma_mapeo_rapido`
3. reconciliar `legislacion` extraida con `norma_mapeo_legislacion_extraida`
4. mover la busqueda y el ranking hacia `norma_fragmentos`

## Recomendacion practica

Para leyes simples o pruebas pequenas, sigue usando la via rapida. Para codigos completos, reformas historicas y consolidacion seria, empieza a cargar datos en la via lenta desde ya.
