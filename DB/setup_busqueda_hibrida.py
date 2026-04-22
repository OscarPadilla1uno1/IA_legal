"""Configura BM25 nativo y triggers tsvector para jurisprudencia y leyes."""
import sys

import psycopg2

sys.stdout.reconfigure(encoding="utf-8")

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}


def main():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()

    print("1. Asegurando columnas tsvector...")
    cursor.execute(
        """
        ALTER TABLE fragmentos_texto
        ADD COLUMN IF NOT EXISTS tsv_contenido tsvector;

        ALTER TABLE fragmentos_normativos
        ADD COLUMN IF NOT EXISTS tsv_contenido tsvector;
        """
    )
    print("   Columnas listas.")

    print("2. Creando función compartida...")
    cursor.execute(
        """
        CREATE OR REPLACE FUNCTION update_tsv_generic()
        RETURNS trigger AS $$
        BEGIN
            NEW.tsv_contenido := to_tsvector('spanish', COALESCE(NEW.contenido, ''));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    print("   Función creada.")

    print("3. Poblando tsvector existente...")
    cursor.execute(
        """
        UPDATE fragmentos_texto
        SET tsv_contenido = to_tsvector('spanish', COALESCE(contenido, ''))
        WHERE tsv_contenido IS NULL;
        """
    )
    print(f"   fragmentos_texto indexados: {cursor.rowcount}")
    cursor.execute(
        """
        UPDATE fragmentos_normativos
        SET tsv_contenido = to_tsvector('spanish', COALESCE(contenido, ''))
        WHERE tsv_contenido IS NULL;
        """
    )
    print(f"   fragmentos_normativos indexados: {cursor.rowcount}")

    print("4. Construyendo índices GIN...")
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fragmentos_tsv
        ON fragmentos_texto USING gin(tsv_contenido);

        CREATE INDEX IF NOT EXISTS idx_fragmentos_normativos_tsv
        ON fragmentos_normativos USING gin(tsv_contenido);
        """
    )
    print("   Índices GIN activos.")

    print("5. Creando triggers automáticos...")
    cursor.execute(
        """
        DROP TRIGGER IF EXISTS trg_tsv_contenido ON fragmentos_texto;
        CREATE TRIGGER trg_tsv_contenido
        BEFORE INSERT OR UPDATE OF contenido ON fragmentos_texto
        FOR EACH ROW EXECUTE FUNCTION update_tsv_generic();

        DROP TRIGGER IF EXISTS trg_tsv_normativo ON fragmentos_normativos;
        CREATE TRIGGER trg_tsv_normativo
        BEFORE INSERT OR UPDATE OF contenido ON fragmentos_normativos
        FOR EACH ROW EXECUTE FUNCTION update_tsv_generic();
        """
    )
    print("   Triggers instalados.")

    print("\nBúsqueda híbrida configurada para jurisprudencia y leyes.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
