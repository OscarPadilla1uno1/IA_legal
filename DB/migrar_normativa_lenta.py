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
    schema_path = base_dir / "schema_normativa_lenta.sql"
    ddl = schema_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        ejecutar(cursor, ddl, "1. Aplicando esquema de via lenta normativa...")

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
            "2. Asegurando funcion compartida para tsvector...",
        )

        ejecutar(
            cursor,
            """
            DROP TRIGGER IF EXISTS trg_tsv_norma_fragmentos ON norma_fragmentos;
            CREATE TRIGGER trg_tsv_norma_fragmentos
            BEFORE INSERT OR UPDATE OF contenido ON norma_fragmentos
            FOR EACH ROW EXECUTE FUNCTION update_tsv_generic();
            """,
            "3. Instalando trigger de busqueda textual para fragmentos lentos...",
        )

        ejecutar(
            cursor,
            """
            UPDATE norma_fragmentos
            SET tsv_contenido = to_tsvector('spanish', COALESCE(contenido, ''))
            WHERE tsv_contenido IS NULL;
            """,
            "4. Poblando tsvector existente en via lenta...",
        )

        ejecutar(
            cursor,
            """
            CREATE INDEX IF NOT EXISTS norma_fragmentos_hnsw_idx
            ON norma_fragmentos
            USING hnsw (embedding_fragmento vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
            """,
            "5. Asegurando indice vectorial HNSW en via lenta...",
        )

        ejecutar(
            cursor,
            """
            INSERT INTO norma_tipos_documento (codigo, nombre, descripcion)
            VALUES
                ('constitucion', 'Constitucion', 'Norma constitucional'),
                ('codigo', 'Codigo', 'Codigo principal o especializado'),
                ('ley', 'Ley', 'Ley ordinaria o especial'),
                ('decreto', 'Decreto', 'Decreto legislativo o ejecutivo'),
                ('reglamento', 'Reglamento', 'Reglamento o disposicion reglamentaria'),
                ('acuerdo', 'Acuerdo', 'Acuerdo administrativo'),
                ('fe_errata', 'Fe de errata', 'Correccion oficial'),
                ('reforma', 'Reforma', 'Documento de reforma normativa'),
                ('tratado', 'Tratado', 'Tratado o convenio internacional')
            ON CONFLICT (codigo) DO UPDATE
            SET nombre = EXCLUDED.nombre,
                descripcion = EXCLUDED.descripcion;

            INSERT INTO norma_fuentes (codigo, nombre, base_url, tipo_fuente, metadata)
            VALUES
                ('cedij', 'CEDIJ Poder Judicial', 'https://legislacion.poderjudicial.gob.hn', 'portal_judicial', '{}'::jsonb),
                ('tsc', 'Biblioteca Virtual TSC', 'https://www.tsc.gob.hn', 'biblioteca', '{}'::jsonb),
                ('lagaceta', 'La Gaceta', 'https://www.lagaceta.gob.hn', 'diario_oficial', '{}'::jsonb)
            ON CONFLICT (codigo) DO UPDATE
            SET nombre = EXCLUDED.nombre,
                base_url = EXCLUDED.base_url,
                tipo_fuente = EXCLUDED.tipo_fuente,
                metadata = EXCLUDED.metadata;
            """,
            "6. Sembrando catalogos base para documentos y fuentes...",
        )

        cursor.execute("SELECT COUNT(*) FROM norma_documentos")
        documentos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM norma_versiones")
        versiones = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM norma_nodos")
        nodos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM norma_fragmentos")
        fragmentos = cursor.fetchone()[0]

        print("\nMigracion via lenta completada.")
        print(f"Documentos normativos: {documentos}")
        print(f"Versiones normativas: {versiones}")
        print(f"Nodos normativos lentos: {nodos}")
        print(f"Fragmentos normativos lentos: {fragmentos}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
