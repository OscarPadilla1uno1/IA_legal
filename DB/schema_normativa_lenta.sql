CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS norma_tipos_documento (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    codigo TEXT NOT NULL UNIQUE,
    nombre TEXT NOT NULL,
    descripcion TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_fuentes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    codigo TEXT UNIQUE,
    nombre TEXT NOT NULL,
    base_url TEXT,
    tipo_fuente TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_documentos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ref_carga TEXT UNIQUE,
    legislacion_id UUID REFERENCES legislaciones(id) ON DELETE SET NULL,
    tipo_documento_id UUID NOT NULL REFERENCES norma_tipos_documento(id) ON DELETE RESTRICT,
    documento_padre_id UUID REFERENCES norma_documentos(id) ON DELETE SET NULL,
    nombre_oficial TEXT NOT NULL,
    nombre_corto TEXT,
    sigla TEXT,
    numero_oficial TEXT,
    autoridad_emisora TEXT,
    ambito TEXT,
    estado_vigencia TEXT DEFAULT 'vigente',
    fecha_emision DATE,
    fecha_publicacion DATE,
    fecha_vigencia DATE,
    fecha_derogacion DATE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_aliases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    documento_id UUID NOT NULL REFERENCES norma_documentos(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    alias_normalizado TEXT NOT NULL,
    es_principal BOOLEAN DEFAULT FALSE,
    UNIQUE (documento_id, alias_normalizado)
);

CREATE TABLE IF NOT EXISTS norma_versiones (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    documento_id UUID NOT NULL REFERENCES norma_documentos(id) ON DELETE CASCADE,
    ref_carga TEXT UNIQUE,
    version_label TEXT,
    numero_version TEXT,
    fecha_inicio_vigencia DATE,
    fecha_fin_vigencia DATE,
    es_vigente BOOLEAN DEFAULT TRUE,
    texto_consolidado TEXT,
    hash_texto TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_version_fuentes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    version_id UUID NOT NULL REFERENCES norma_versiones(id) ON DELETE CASCADE,
    fuente_id UUID REFERENCES norma_fuentes(id) ON DELETE SET NULL,
    fuente_url TEXT,
    ruta_archivo TEXT,
    checksum_archivo TEXT,
    es_principal BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_nodos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    version_id UUID NOT NULL REFERENCES norma_versiones(id) ON DELETE CASCADE,
    nodo_padre_id UUID REFERENCES norma_nodos(id) ON DELETE CASCADE,
    ref_carga TEXT UNIQUE,
    tipo_nodo TEXT NOT NULL,
    etiqueta TEXT NOT NULL,
    identificador TEXT,
    titulo TEXT,
    articulo_numero TEXT,
    texto TEXT,
    texto_normalizado TEXT,
    orden INTEGER,
    profundidad INTEGER DEFAULT 0,
    path_text TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_fragmentos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    documento_id UUID NOT NULL REFERENCES norma_documentos(id) ON DELETE CASCADE,
    version_id UUID NOT NULL REFERENCES norma_versiones(id) ON DELETE CASCADE,
    nodo_id UUID NOT NULL REFERENCES norma_nodos(id) ON DELETE CASCADE,
    tipo_fragmento TEXT NOT NULL,
    contenido TEXT NOT NULL,
    orden INTEGER,
    embedding_fragmento VECTOR(1024),
    tsv_contenido tsvector,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_relaciones (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    documento_origen_id UUID NOT NULL REFERENCES norma_documentos(id) ON DELETE CASCADE,
    documento_destino_id UUID NOT NULL REFERENCES norma_documentos(id) ON DELETE CASCADE,
    tipo_relacion TEXT NOT NULL,
    version_origen_id UUID REFERENCES norma_versiones(id) ON DELETE SET NULL,
    version_destino_id UUID REFERENCES norma_versiones(id) ON DELETE SET NULL,
    nodo_origen_id UUID REFERENCES norma_nodos(id) ON DELETE SET NULL,
    nodo_destino_id UUID REFERENCES norma_nodos(id) ON DELETE SET NULL,
    fecha_relacion DATE,
    notas TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_mapeo_rapido (
    ley_id UUID PRIMARY KEY REFERENCES leyes(id) ON DELETE CASCADE,
    documento_id UUID REFERENCES norma_documentos(id) ON DELETE SET NULL,
    version_id UUID REFERENCES norma_versiones(id) ON DELETE SET NULL,
    nodo_articulo_id UUID REFERENCES norma_nodos(id) ON DELETE SET NULL,
    confianza NUMERIC(5,4),
    estado_match TEXT DEFAULT 'pendiente',
    notas TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS norma_mapeo_legislacion_extraida (
    legislacion_id UUID PRIMARY KEY REFERENCES legislacion(id) ON DELETE CASCADE,
    documento_id UUID REFERENCES norma_documentos(id) ON DELETE SET NULL,
    nodo_articulo_id UUID REFERENCES norma_nodos(id) ON DELETE SET NULL,
    confianza NUMERIC(5,4),
    estado_match TEXT DEFAULT 'pendiente',
    notas TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_norma_tipos_documento_codigo
    ON norma_tipos_documento (codigo);

CREATE INDEX IF NOT EXISTS idx_norma_fuentes_codigo
    ON norma_fuentes (codigo);

CREATE INDEX IF NOT EXISTS idx_norma_documentos_nombre_oficial
    ON norma_documentos (LOWER(nombre_oficial));

CREATE INDEX IF NOT EXISTS idx_norma_documentos_nombre_corto
    ON norma_documentos (LOWER(nombre_corto));

CREATE INDEX IF NOT EXISTS idx_norma_documentos_sigla
    ON norma_documentos (LOWER(sigla));

CREATE INDEX IF NOT EXISTS idx_norma_documentos_tipo
    ON norma_documentos (tipo_documento_id);

CREATE INDEX IF NOT EXISTS idx_norma_documentos_legislacion
    ON norma_documentos (legislacion_id);

CREATE INDEX IF NOT EXISTS idx_norma_aliases_alias_normalizado
    ON norma_aliases (alias_normalizado);

CREATE INDEX IF NOT EXISTS idx_norma_versiones_documento
    ON norma_versiones (documento_id);

CREATE INDEX IF NOT EXISTS idx_norma_nodos_version_orden
    ON norma_nodos (version_id, tipo_nodo, orden);

CREATE INDEX IF NOT EXISTS idx_norma_nodos_padre
    ON norma_nodos (nodo_padre_id);

CREATE INDEX IF NOT EXISTS idx_norma_nodos_articulo_numero
    ON norma_nodos (articulo_numero);

CREATE INDEX IF NOT EXISTS idx_norma_nodos_path_text
    ON norma_nodos (path_text);

CREATE INDEX IF NOT EXISTS idx_norma_fragmentos_tsv
    ON norma_fragmentos USING gin (tsv_contenido);

CREATE INDEX IF NOT EXISTS idx_norma_fragmentos_nodo
    ON norma_fragmentos (nodo_id);

CREATE INDEX IF NOT EXISTS idx_norma_fragmentos_documento
    ON norma_fragmentos (documento_id);

CREATE INDEX IF NOT EXISTS idx_norma_relaciones_origen_tipo
    ON norma_relaciones (documento_origen_id, tipo_relacion);

CREATE INDEX IF NOT EXISTS idx_norma_relaciones_destino_tipo
    ON norma_relaciones (documento_destino_id, tipo_relacion);
