import sys

import psycopg2

from db_config import describe_db_target
from dashboard_busqueda import LegalSearchService

sys.stdout.reconfigure(encoding="utf-8")


def imprimir_fuentes(results):
    print("\nFuentes consultadas:")
    for index, item in enumerate(results, start=1):
        if item["source_type"] == "jurisprudencia":
            print(f"  [{index}] Jurisprudencia | {item['sentencia']}")
            print(f"      Fecha: {item['fecha'] or 'S/F'} | Tipo: {item['tipo']}")
        else:
            print(f"  [{index}] Ley | {item['ley']} | {item['articulo']}")
            print(f"      Versión: {item.get('version') or 'S/V'} | Fecha: {item['fecha'] or 'S/F'}")
        print(
            "      "
            f"Reranker: {item.get('score_reranker', 0):.4f} | "
            f"Vector: {item.get('sim_vector', 0):.4f} | "
            f"BM25: {item.get('rank_bm25', 0):.4f}"
        )


def main():
    service = LegalSearchService()

    print("=" * 80)
    print("  ASISTENTE JURÍDICO IA")
    print("  Corpus activo por defecto: jurisprudencia + leyes canónicas")
    print("=" * 80)

    while True:
        pregunta = input("\nTu consulta jurídica (o 'salir'): ").strip()
        if not pregunta or pregunta.lower() in {"salir", "exit", "q"}:
            print("\nHasta luego.")
            break

        try:
            respuesta = service.llm_search(
                question=pregunta,
                top_k_retrieve=10,
                top_k_final=5,
                corpus="ambos",
            )
        except psycopg2.OperationalError as exc:
            print("\nNo se pudo conectar a PostgreSQL.")
            print(f"Destino configurado: {describe_db_target()}")
            print("Configura DATABASE_URL o las variables DB_NAME, DB_USER, DB_PASSWORD, DB_HOST y DB_PORT.")
            print(f"Detalle: {exc}")
            continue

        print("\n" + "─" * 80)
        print("RESPUESTA")
        print("─" * 80)
        print(respuesta["answer"])
        print("─" * 80)

        if respuesta["results"]:
            imprimir_fuentes(respuesta["results"])
        else:
            print("\nNo se encontraron fragmentos relevantes.")


if __name__ == "__main__":
    main()
