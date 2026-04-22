import sys

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

        respuesta = service.llm_search(
            question=pregunta,
            top_k_retrieve=10,
            top_k_final=5,
            corpus="ambos",
        )

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
