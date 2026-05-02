"""
Valida en la base de datos el accuracy del modelo guardado.

Qué hace:
- Carga `modelos_ml/clasificador_fallos.pkl`
- Reconstruye las features desde PostgreSQL con la misma lógica de `ml_clasificador.py`
- Reproduce el mismo split determinista train/test
- Evalúa el modelo guardado sobre el holdout y compara contra la métrica guardada
"""

from __future__ import annotations

import os
import pickle
import sys

from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import train_test_split

from ml_clasificador import RANDOM_STATE, cargar_dataset, info, ok, titulo, warn


MODELO_PATH = os.path.join(os.path.dirname(__file__), "modelos_ml", "clasificador_fallos.pkl")
TEST_SIZE = 0.20


def cargar_artefacto(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el artefacto: {path}")
    with open(path, "rb") as archivo:
        return pickle.load(archivo)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    titulo("VALIDACIÓN DE ACCURACY DESDE LA DB")

    artefacto = cargar_artefacto(MODELO_PATH)
    bundle = cargar_dataset()

    if artefacto.get("uses_fallo_text"):
        warn("Este modelo usa `s.fallo` como señal léxica fuerte; la validación confirma ese escenario exacto.")

    X_full = bundle.X_clasif
    y = bundle.y
    label_encoder = artefacto["label_encoder"]
    y_enc = label_encoder.transform(y)

    _, X_test, _, y_test = train_test_split(
        X_full,
        y_enc,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y_enc,
    )

    modelo = artefacto["modelo"]
    y_pred = modelo.predict(X_test)

    acc = accuracy_score(y_test, y_pred) * 100
    f1_macro = f1_score(y_test, y_pred, average="macro") * 100
    f1_weighted = f1_score(y_test, y_pred, average="weighted") * 100

    accuracy_guardada = float(artefacto.get("accuracy", 0.0))
    delta = abs(acc - accuracy_guardada)

    info(f"Artefacto: {MODELO_PATH}")
    info(f"Modelo: {artefacto.get('nombre')}")
    info(f"Dimensión de features esperada: {artefacto.get('classification_dim')}")
    info(f"Dimensión de features reconstruida: {X_full.shape[1]}")

    print(f"\n  Accuracy medido   : {acc:.6f}%")
    print(f"  Accuracy guardado : {accuracy_guardada:.6f}%")
    print(f"  Delta absoluto    : {delta:.10f}")
    print(f"  F1 macro          : {f1_macro:.6f}%")
    print(f"  F1 weighted       : {f1_weighted:.6f}%")

    print("\nReporte por clase:")
    reporte = classification_report(y_test, y_pred, target_names=label_encoder.classes_, digits=3)
    for linea in reporte.split("\n"):
        print(f"  {linea}")

    if delta < 1e-6:
        ok("La validación coincide exactamente con el accuracy almacenado.")
    elif delta < 0.01:
        ok("La validación coincide prácticamente con el accuracy almacenado.")
    else:
        warn("La validación difiere del accuracy almacenado; revisa si cambió el artefacto o el feature pipeline.")


if __name__ == "__main__":
    main()
