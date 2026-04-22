import time

import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}


def create_index(cursor, sql, label):
    print(f"Creando índice HNSW para {label}...")
    t0 = time.time()
    cursor.execute(sql)
    print(f"Índice {label} listo en {(time.time() - t0) / 60:.2f} minutos.")


def main():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        create_index(
            cursor,
            """
            CREATE INDEX IF NOT EXISTS fragmentos_hnsw_idx
            ON fragmentos_texto
            USING hnsw (embedding_fragmento vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
            """,
            "jurisprudencia",
        )
        create_index(
            cursor,
            """
            CREATE INDEX IF NOT EXISTS fragmentos_normativos_hnsw_idx
            ON fragmentos_normativos
            USING hnsw (embedding_fragmento vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
            """,
            "normativa",
        )
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
