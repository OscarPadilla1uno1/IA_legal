"""
Orquestador de vectorización completa:
  1) Reanuda vectorización de fragmentos_texto (columna embedding_fragmento)
  2) Vectoriza sentencias (columna embedding)
Ambos usando BAAI/bge-m3 con PyTorch nativo + CUDA.
"""
import subprocess
import sys
import time

PYTHON = sys.executable


def run_step(nombre, script):
    print("\n" + "=" * 60)
    print(f"  PASO: {nombre}")
    print("=" * 60)
    t0 = time.time()
    result = subprocess.run(
        [PYTHON, script],
        cwd=r"c:\Project\DB",
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    elapsed = (time.time() - t0) / 60
    if result.returncode != 0:
        print(f"\n[ERROR] {nombre} terminó con código {result.returncode}")
        print(f"Tiempo: {elapsed:.2f} min")
        return False
    print(f"\n[OK] {nombre} completado en {elapsed:.2f} min")
    return True


if __name__ == "__main__":
    t_inicio = time.time()

    ok1 = run_step(
        "Fragmentos de texto (fragmentos_texto.embedding_fragmento)",
        "vectorizador_bge_nativo.py",
    )

    ok2 = run_step(
        "Sentencias íntegras (sentencias.embedding)",
        "vectorizador_sentencias_bge_nativo.py",
    )

    total = (time.time() - t_inicio) / 60
    print("\n" + "=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)
    print(f"  fragmentos_texto : {'OK' if ok1 else 'ERROR'}")
    print(f"  sentencias       : {'OK' if ok2 else 'ERROR'}")
    print(f"  Tiempo total     : {total:.2f} min")
