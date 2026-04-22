-- 1. Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Creación de Catálogos / Tablas Independientes
CREATE TABLE IF NOT EXISTS tipos_proceso (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre TEXT,
    subtipo TEXT
);

CREATE TABLE IF NOT EXISTS magistrados (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre TEXT,
    cargo TEXT,
    activo BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS tribunales (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre TEXT
);

CREATE TABLE IF NOT EXISTS personas_entidades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre TEXT NOT NULL,
    tipo TEXT,
    rol_habitual TEXT
);

CREATE TABLE IF NOT EXISTS materias (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    materia_padre_id UUID REFERENCES materias(id) ON DELETE CASCADE,
    nombre TEXT
);

CREATE TABLE IF NOT EXISTS legislacion (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre_ley TEXT,
    articulo TEXT,
    sub_indice TEXT
);

CREATE TABLE IF NOT EXISTS legislaciones (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    codigo TEXT,
    nombre TEXT NOT NULL,
    descripcion TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leyes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    legislacion_id UUID REFERENCES legislaciones(id) ON DELETE SET NULL,
    tipo_norma TEXT NOT NULL DEFAULT 'ley',
    nombre_oficial TEXT NOT NULL,
    nombre_corto TEXT,
    sigla TEXT,
    ambito TEXT,
    autoridad_emisora TEXT,
    estado_vigencia TEXT DEFAULT 'vigente',
    fecha_publicacion DATE,
    fecha_vigencia DATE,
    fecha_derogacion DATE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS leyes_alias (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ley_id UUID NOT NULL REFERENCES leyes(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    alias_normalizado TEXT NOT NULL,
    es_principal BOOLEAN DEFAULT FALSE,
    UNIQUE (ley_id, alias_normalizado)
);

CREATE TABLE IF NOT EXISTS leyes_versiones (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ley_id UUID NOT NULL REFERENCES leyes(id) ON DELETE CASCADE,
    version_label TEXT,
    fecha_inicio_vigencia DATE,
    fecha_fin_vigencia DATE,
    es_vigente BOOLEAN DEFAULT TRUE,
    texto_consolidado TEXT,
    fuente_url TEXT,
    notas TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS articulos_ley (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ley_version_id UUID NOT NULL REFERENCES leyes_versiones(id) ON DELETE CASCADE,
    articulo_numero TEXT NOT NULL,
    articulo_etiqueta TEXT NOT NULL,
    numeral TEXT,
    literal TEXT,
    rubro TEXT,
    texto_oficial TEXT NOT NULL,
    texto_normalizado TEXT,
    orden INTEGER,
    hash_contenido TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nodos_normativos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ley_version_id UUID NOT NULL REFERENCES leyes_versiones(id) ON DELETE CASCADE,
    nodo_padre_id UUID REFERENCES nodos_normativos(id) ON DELETE CASCADE,
    tipo_nodo TEXT NOT NULL,
    etiqueta TEXT NOT NULL,
    identificador TEXT,
    titulo TEXT,
    texto TEXT,
    texto_normalizado TEXT,
    orden INTEGER,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tabla Central (Sentencias)
CREATE TABLE IF NOT EXISTS sentencias (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    numero_sentencia TEXT,
    fecha_resolucion DATE,
    fecha_sentencia_recurrida DATE,
    fallo TEXT,
    anonimizada BOOLEAN DEFAULT FALSE,
    texto_integro TEXT,
    vigencia_jurisprudencial TEXT,
    jerarquia_jurisprudencial TEXT,
    tiene_novedades BOOLEAN DEFAULT FALSE,
    embedding VECTOR(1024),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    -- Llaves Foráneas Base
    tipo_proceso_id UUID REFERENCES tipos_proceso(id) ON DELETE SET NULL,
    magistrado_id UUID REFERENCES magistrados(id) ON DELETE SET NULL,
    tribunal_id UUID REFERENCES tribunales(id) ON DELETE SET NULL,
    recurrente_id UUID REFERENCES personas_entidades(id) ON DELETE SET NULL,
    recurrido_id UUID REFERENCES personas_entidades(id) ON DELETE SET NULL
);

-- 4. Tablas Pivote (N:M)
CREATE TABLE IF NOT EXISTS sentencias_materias (
    sentencia_id UUID REFERENCES sentencias(id) ON DELETE CASCADE,
    materia_id UUID REFERENCES materias(id) ON DELETE CASCADE,
    PRIMARY KEY (sentencia_id, materia_id)
);

CREATE TABLE IF NOT EXISTS sentencias_legislacion (
    sentencia_id UUID REFERENCES sentencias(id) ON DELETE CASCADE,
    legislacion_id UUID REFERENCES legislacion(id) ON DELETE CASCADE,
    PRIMARY KEY (sentencia_id, legislacion_id)
);

CREATE TABLE IF NOT EXISTS sentencias_articulos_ley (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sentencia_id UUID NOT NULL REFERENCES sentencias(id) ON DELETE CASCADE,
    articulo_ley_id UUID NOT NULL REFERENCES articulos_ley(id) ON DELETE CASCADE,
    fuente_extraccion TEXT DEFAULT 'reconciliacion',
    alias_detectado TEXT,
    confianza NUMERIC(5,4),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sentencia_id, articulo_ley_id)
);

CREATE TABLE IF NOT EXISTS mapeo_legislacion_canonica (
    legislacion_id UUID PRIMARY KEY REFERENCES legislacion(id) ON DELETE CASCADE,
    ley_id UUID REFERENCES leyes(id) ON DELETE SET NULL,
    articulo_ley_id UUID REFERENCES articulos_ley(id) ON DELETE SET NULL,
    alias_detectado TEXT,
    confianza NUMERIC(5,4),
    estado_match TEXT DEFAULT 'pendiente',
    notas TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5. Tablas Hijas y Módulos de IA (RAG)
CREATE TABLE IF NOT EXISTS tesauro (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sentencia_id UUID REFERENCES sentencias(id) ON DELETE CASCADE,
    categoria TEXT,
    subcategoria TEXT,
    problema_juridico TEXT,
    respuesta_juridica TEXT
);

CREATE TABLE IF NOT EXISTS fragmentos_texto (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sentencia_id UUID REFERENCES sentencias(id) ON DELETE CASCADE,
    tipo_fragmento TEXT,
    contenido TEXT,
    orden INTEGER,
    embedding_fragmento VECTOR(1024),
    tsv_contenido tsvector
);

CREATE TABLE IF NOT EXISTS fragmentos_normativos (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ley_id UUID NOT NULL REFERENCES leyes(id) ON DELETE CASCADE,
    ley_version_id UUID NOT NULL REFERENCES leyes_versiones(id) ON DELETE CASCADE,
    articulo_id UUID REFERENCES articulos_ley(id) ON DELETE CASCADE,
    nodo_id UUID REFERENCES nodos_normativos(id) ON DELETE CASCADE,
    tipo_fragmento TEXT NOT NULL,
    contenido TEXT NOT NULL,
    orden INTEGER,
    embedding_fragmento VECTOR(1024),
    tsv_contenido tsvector,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS documentos_pdf (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sentencia_id UUID REFERENCES sentencias(id) ON DELETE CASCADE,
    ruta_archivo TEXT
);

CREATE INDEX IF NOT EXISTS idx_legislaciones_nombre ON legislaciones (LOWER(nombre));
CREATE INDEX IF NOT EXISTS idx_leyes_nombre_oficial ON leyes (LOWER(nombre_oficial));
CREATE INDEX IF NOT EXISTS idx_leyes_nombre_corto ON leyes (LOWER(nombre_corto));
CREATE INDEX IF NOT EXISTS idx_leyes_sigla ON leyes (LOWER(sigla));
CREATE INDEX IF NOT EXISTS idx_leyes_tipo_norma ON leyes (tipo_norma);
CREATE INDEX IF NOT EXISTS idx_leyes_alias_alias_normalizado ON leyes_alias (alias_normalizado);
CREATE INDEX IF NOT EXISTS idx_leyes_versiones_ley_id ON leyes_versiones (ley_id);
CREATE INDEX IF NOT EXISTS idx_articulos_ley_version_orden ON articulos_ley (ley_version_id, orden);
CREATE UNIQUE INDEX IF NOT EXISTS idx_articulos_ley_unicos
    ON articulos_ley (ley_version_id, articulo_etiqueta, COALESCE(numeral, ''), COALESCE(literal, ''));
CREATE INDEX IF NOT EXISTS idx_articulos_ley_numero ON articulos_ley (articulo_numero);
CREATE INDEX IF NOT EXISTS idx_nodos_normativos_version_orden ON nodos_normativos (ley_version_id, tipo_nodo, orden);
CREATE INDEX IF NOT EXISTS idx_nodos_normativos_padre ON nodos_normativos (nodo_padre_id);
CREATE INDEX IF NOT EXISTS idx_fragmentos_texto_tsv ON fragmentos_texto USING gin (tsv_contenido);
CREATE INDEX IF NOT EXISTS idx_fragmentos_normativos_tsv ON fragmentos_normativos USING gin (tsv_contenido);
CREATE INDEX IF NOT EXISTS idx_fragmentos_normativos_articulo ON fragmentos_normativos (articulo_id);
CREATE INDEX IF NOT EXISTS idx_fragmentos_normativos_nodo ON fragmentos_normativos (nodo_id);
CREATE INDEX IF NOT EXISTS idx_sentencias_articulos_ley_sentencia ON sentencias_articulos_ley (sentencia_id);
CREATE INDEX IF NOT EXISTS idx_sentencias_articulos_ley_articulo ON sentencias_articulos_ley (articulo_ley_id);
CREATE INDEX IF NOT EXISTS idx_mapeo_legislacion_canonica_ley ON mapeo_legislacion_canonica (ley_id);
