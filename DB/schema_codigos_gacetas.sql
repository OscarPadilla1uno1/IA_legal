-- Extensiones necesarias (ya existen, pero por seguridad)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

-- ==========================================
-- ESTRUCTURA PARA CÓDIGOS (Niveles Múltiples)
-- ==========================================

CREATE TABLE IF NOT EXISTS codigos_honduras (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nombre_oficial TEXT NOT NULL,
    descripcion TEXT,
    fecha_promulgacion DATE,
    fecha_vigencia DATE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Nivel Macroscópico: CAPÍTULOS (o Títulos/Libros aglomerados)
CREATE TABLE IF NOT EXISTS capitulos_codigo (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    codigo_id UUID NOT NULL REFERENCES codigos_honduras(id) ON DELETE CASCADE,
    libro_etiqueta TEXT,
    titulo_etiqueta TEXT,
    capitulo_etiqueta TEXT NOT NULL,
    texto_aglomerado TEXT NOT NULL,   -- Todo el texto que comprende el capítulo para alto contexto
    orden INTEGER,
    embedding VECTOR(1024),           -- Vector de alto nivel!
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Nivel Microscópico: ARTÍCULOS
CREATE TABLE IF NOT EXISTS articulos_codigo (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    capitulo_id UUID NOT NULL REFERENCES capitulos_codigo(id) ON DELETE CASCADE,
    codigo_id UUID NOT NULL REFERENCES codigos_honduras(id) ON DELETE CASCADE,
    articulo_numero TEXT NOT NULL,
    articulo_etiqueta TEXT NOT NULL,
    texto_oficial TEXT NOT NULL,
    orden INTEGER,
    embedding VECTOR(1024),           -- Vector de bajo nivel!
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Índices HNSW (Hierarchical Navigable Small World) para pgvector 
-- Optimizan la búsqueda de la distancia coseno
CREATE INDEX IF NOT EXISTS idx_capitulos_embedding ON capitulos_codigo USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_articulos_codigo_embedding ON articulos_codigo USING hnsw (embedding vector_cosine_ops);


-- ==========================================
-- ESTRUCTURA PARA GACETAS OFICIALES
-- ==========================================

CREATE TABLE IF NOT EXISTS gacetas_oficiales (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    numero_edicion TEXT,
    fecha_publicacion DATE,
    url_fuente TEXT,
    texto_integro TEXT NOT NULL,
    embedding VECTOR(1024),           -- Vector de La Gaceta
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gacetas_embedding ON gacetas_oficiales USING hnsw (embedding vector_cosine_ops);

-- Privilegios (Opcional, en caso que existan usuarios en la BD)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;
