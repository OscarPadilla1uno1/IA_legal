import sys
from pathlib import Path

import psycopg2

sys.stdout.reconfigure(encoding="utf-8")

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}


def ejecutar(cursor, sql, mensaje):
    print(mensaje)
    cursor.execute(sql)


def main():
    base_dir = Path(__file__).resolve().parent
    schema_path = base_dir / "schema.sql"
    ddl = schema_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        ejecutar(cursor, ddl, "1. Aplicando esquema base y corpus legal canonico...")

        ejecutar(
            cursor,
            """
            ALTER TABLE leyes
            ADD COLUMN IF NOT EXISTS tipo_norma TEXT NOT NULL DEFAULT 'ley';

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

            ALTER TABLE fragmentos_normativos
            ADD COLUMN IF NOT EXISTS nodo_id UUID REFERENCES nodos_normativos(id) ON DELETE CASCADE;

            CREATE INDEX IF NOT EXISTS idx_leyes_tipo_norma ON leyes (tipo_norma);
            CREATE INDEX IF NOT EXISTS idx_nodos_normativos_version_orden ON nodos_normativos (ley_version_id, tipo_nodo, orden);
            CREATE INDEX IF NOT EXISTS idx_nodos_normativos_padre ON nodos_normativos (nodo_padre_id);
            CREATE INDEX IF NOT EXISTS idx_fragmentos_normativos_nodo ON fragmentos_normativos (nodo_id);
            """,
            "1.1. Ajustando tablas existentes para soportar codigos y nodos jerarquicos...",
        )

        ejecutar(
            cursor,
            """
            CREATE OR REPLACE FUNCTION update_tsv_generic()
            RETURNS trigger AS $$
            BEGIN
                NEW.tsv_contenido := to_tsvector('spanish', COALESCE(NEW.contenido, ''));
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            "2. Creando funcion compartida para tsvector...",
        )

        ejecutar(
            cursor,
            """
            DROP TRIGGER IF EXISTS trg_tsv_contenido ON fragmentos_texto;
            CREATE TRIGGER trg_tsv_contenido
            BEFORE INSERT OR UPDATE OF contenido ON fragmentos_texto
            FOR EACH ROW EXECUTE FUNCTION update_tsv_generic();

            DROP TRIGGER IF EXISTS trg_tsv_normativo ON fragmentos_normativos;
            CREATE TRIGGER trg_tsv_normativo
            BEFORE INSERT OR UPDATE OF contenido ON fragmentos_normativos
            FOR EACH ROW EXECUTE FUNCTION update_tsv_generic();
            """,
            "3. Instalando triggers automaticos para busqueda textual...",
        )

        ejecutar(
            cursor,
            """
            UPDATE fragmentos_texto
            SET tsv_contenido = to_tsvector('spanish', COALESCE(contenido, ''))
            WHERE tsv_contenido IS NULL;

            UPDATE fragmentos_normativos
            SET tsv_contenido = to_tsvector('spanish', COALESCE(contenido, ''))
            WHERE tsv_contenido IS NULL;
            """,
            "4. Poblando tsvector existente...",
        )

        ejecutar(
            cursor,
            """
            CREATE INDEX IF NOT EXISTS fragmentos_hnsw_idx
            ON fragmentos_texto
            USING hnsw (embedding_fragmento vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);

            CREATE INDEX IF NOT EXISTS fragmentos_normativos_hnsw_idx
            ON fragmentos_normativos
            USING hnsw (embedding_fragmento vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
            """,
            "5. Asegurando indices vectoriales HNSW...",
        )

        cursor.execute("SELECT COUNT(*) FROM leyes")
        leyes = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM articulos_ley")
        articulos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM nodos_normativos")
        nodos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM fragmentos_normativos")
        fragmentos = cursor.fetchone()[0]

        print("\nMigracion completada.")
        print(f"Leyes canonicas: {leyes}")
        print(f"Articulos canonicos: {articulos}")
        print(f"Nodos normativos: {nodos}")
        print(f"Fragmentos normativos: {fragmentos}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
