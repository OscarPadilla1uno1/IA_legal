"""
Clasificador de fallo_macro orientado a precision alta.

Estrategia:
- Metadatos completos: materias, tipo_proceso y top-20 tribunales
- Keywords juridicas desde `fallo` y el tramo resolutivo
- Comparativa de base learners vs StackingClassifier

Nota importante:
`fallo_macro` fue derivado historicamente de `fallo`, asi que usar `fallo`
como fuente de keywords es una señal muy fuerte y puede comportarse como
"casi-etiqueta". Este enfoque solo es valido si ese texto existe tambien
en inferencia.
"""

from __future__ import annotations

import os
import pickle
import re
import sys
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass

import numpy as np
import psycopg2
from sklearn.base import clone
from sklearn.calibration import CalibratedClassifierCV
from sklearn.cluster import MiniBatchKMeans
from sklearn.ensemble import StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    adjusted_rand_score,
    classification_report,
    f1_score,
    precision_recall_fscore_support,
    silhouette_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.svm import LinearSVC

sys.stdout.reconfigure(encoding="utf-8")
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")

G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
B = "\033[94m"
C = "\033[96m"
BOLD = "\033[1m"
X = "\033[0m"


def titulo(texto: str) -> None:
    print(f"\n{BOLD}{B}{'═' * 72}{X}")
    print(f"{BOLD}{B}  {texto}{X}")
    print(f"{BOLD}{B}{'═' * 72}{X}\n")


def sub(texto: str) -> None:
    print(f"\n{C}  ▶ {texto}{X}")


def ok(texto: str) -> None:
    print(f"  {G}✅ {texto}{X}")


def warn(texto: str) -> None:
    print(f"  {Y}⚠ {texto}{X}")


def info(texto: str) -> None:
    print(f"     {texto}")


DB = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

MODELOS_DIR = os.path.join(os.path.dirname(__file__), "modelos_ml")
os.makedirs(MODELOS_DIR, exist_ok=True)

TOP_TRIBUNALES_N = 20
RESOLUTIVO_CHARS = 6000
STACKING_CV = 3
CV_FOLDS = 5
RANDOM_STATE = 42

MATERIAS_CAT = [
    "Derecho Constitucional",
    "Derechos Humanos Grupos Vulnerables",
    "Contencioso Administrativo",
    "Derecho Laboral",
    "Derecho Civil",
    "Derecho Penal",
]

PROCESO_CAT = [
    "Amparo",
    "Casación",
    "Habeas Corpus (Exhibición Personal)",
    "Inconstitucionalidad",
    "Recurso",
    "Revisión",
]

KEYWORD_SPECS = [
    ("kw_no_ha_lugar", [r"\bno ha lugar\b", r"\bsin lugar\b", r"\bdeclara(?:r)? sin lugar\b"]),
    ("kw_ha_lugar", [r"(?<!no )\bha lugar\b", r"\bcon lugar\b", r"\bdeclara(?:r)? con lugar\b"]),
    ("kw_casacion", [r"\bcasaci[oó]n\b", r"\bcasa(?:r|do|da)?\b"]),
    ("kw_revoca", [r"\brevoca(?:r|do|da)?\b"]),
    ("kw_confirma", [r"\bconfirma(?:r|do|da)?\b"]),
    ("kw_sobreseimiento", [r"\bsobreseim", r"\bsobresee\b", r"\bsobreseer\b"]),
    ("kw_nulidad", [r"\bnulidad\b", r"\bnulo\b", r"\bnula\b"]),
    ("kw_inadmisibilidad", [r"\binadmis", r"\bno admit", r"\bin limine\b"]),
    ("kw_otorga", [r"\botorga(?:miento)?\b", r"\botorgado\b"]),
    ("kw_deniega", [r"\bdeniega\b", r"\bdenegado\b", r"\bimprocedente\b"]),
    ("kw_competencia", [r"\bcompetente\b", r"\bincompetente\b"]),
    ("kw_constitucionalidad", [r"\binconstitucional", r"\bconstitucionalidad\b"]),
]

RULE_SIGNAL_SPECS = [
    ("regla_admitido", [r"\bha lugar\b", r"\bcon lugar\b", r"\botorgado\b", r"\bprocedente\b"]),
    ("regla_denegado", [r"\bno ha lugar\b", r"\bsin lugar\b", r"\bdenegado\b", r"\bimprocedente\b"]),
    ("regla_sobreseimiento", [r"\bsobreseim", r"\bsobresee\b", r"\bsobreseer\b"]),
    ("regla_nulidad", [r"\bnulidad\b", r"\bnulo\b", r"\bnula\b"]),
    ("regla_casacion", [r"\bcasaci[oó]n\b", r"\bcasa(?:r|do|da)?\b", r"\brevoca\b", r"\breforma fallo\b"]),
    ("regla_otro", [r"\bdesistimiento\b", r"\breserva\b", r"\babstenerse\b"]),
]

KEYWORD_PATTERNS = [
    (nombre, [re.compile(patron, flags=re.IGNORECASE) for patron in patrones])
    for nombre, patrones in KEYWORD_SPECS
]
RULE_SIGNAL_PATTERNS = [
    (nombre, [re.compile(patron, flags=re.IGNORECASE) for patron in patrones])
    for nombre, patrones in RULE_SIGNAL_SPECS
]


@dataclass
class DatasetBundle:
    X_clasif: np.ndarray
    X_embeddings: np.ndarray
    y: np.ndarray
    materias_arr: np.ndarray
    top_tribunales: list[str]
    keyword_feature_names: list[str]


def normalize_for_keywords(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return texto.lower()


def extract_pattern_features(texto: str, pattern_specs) -> list[float]:
    texto_norm = normalize_for_keywords(texto)
    salida: list[float] = []
    for _, patrones in pattern_specs:
        matches = sum(len(patron.findall(texto_norm)) for patron in patrones)
        salida.append(float(matches > 0))
        salida.append(float(matches))
    return salida


def build_keyword_vector(texto_base: str, texto_auxiliar: str) -> np.ndarray:
    # El fallo resumido es la señal principal; el resolutivo queda solo como respaldo.
    texto_principal = texto_base.strip() or texto_auxiliar.strip()
    features = extract_pattern_features(texto_principal, KEYWORD_PATTERNS)
    features.extend(extract_pattern_features(texto_principal, RULE_SIGNAL_PATTERNS))

    uppercase_ratio = 0.0
    letras = [ch for ch in texto_principal if ch.isalpha()]
    if letras:
        uppercase_ratio = sum(1 for ch in letras if ch.isupper()) / len(letras)

    features.extend([
        float(len(texto_principal)),
        float(texto_principal.count("\n")),
        float(uppercase_ratio),
    ])
    return np.array(features, dtype=np.float32)


def keyword_feature_names() -> list[str]:
    nombres = []
    for nombre, _ in KEYWORD_PATTERNS:
        nombres.extend([f"{nombre}_flag", f"{nombre}_count"])
    for nombre, _ in RULE_SIGNAL_PATTERNS:
        nombres.extend([f"{nombre}_flag", f"{nombre}_count"])
    nombres.extend(["texto_char_len", "texto_linebreaks", "texto_upper_ratio"])
    return nombres


def cargar_dataset() -> DatasetBundle:
    titulo("CARGA DE DATOS — Metadatos + Keywords + Embeddings")
    sub("Conectando a PostgreSQL...")
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM sentencias WHERE fallo_macro IS NOT NULL;")
    n_macro = cur.fetchone()[0]
    if n_macro == 0:
        raise RuntimeError("Ejecuta primero normalizar_fallos.py")
    info(f"Sentencias con fallo_macro: {n_macro:,}")
    warn("Las features léxicas usan s.fallo; este clasificador solo es válido si ese texto existe en inferencia.")

    sub("Calculando top-20 tribunales...")
    cur.execute(
        """
        SELECT tribunal_id::text, COUNT(*) AS n
        FROM sentencias
        WHERE tribunal_id IS NOT NULL
        GROUP BY tribunal_id
        ORDER BY n DESC
        LIMIT %s;
        """,
        (TOP_TRIBUNALES_N,),
    )
    top_tribunales = [row[0] for row in cur.fetchall()]
    tribunal_index = {tribunal_id: idx for idx, tribunal_id in enumerate(top_tribunales)}

    sub("Cargando datos base...")
    t0 = time.time()
    cur.execute(
        f"""
        SELECT
            s.id::text,
            s.embedding::text,
            s.fallo_macro,
            COALESCE(s.fallo, '') AS fallo_texto,
            m.nombre AS materia,
            tp.nombre AS tipo_proceso,
            s.tribunal_id::text,
            RIGHT(COALESCE(s.texto_integro, ''), {RESOLUTIVO_CHARS}) AS texto_resolutivo
        FROM sentencias s
        LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
        LEFT JOIN materias m             ON m.id = sm.materia_id
        LEFT JOIN tipos_proceso tp       ON tp.id = s.tipo_proceso_id
        WHERE s.embedding IS NOT NULL
          AND s.fallo_macro IS NOT NULL
          AND s.fallo_macro != 'DESCONOCIDO'
        ORDER BY s.id;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    info(f"Filas relacionales cargadas: {len(rows):,} en {time.time()-t0:.1f}s")

    sentencias: dict[str, dict] = {}
    for sid, emb_text, fallo_macro, fallo_texto, materia, tipo_proceso, tribunal_id, texto_resolutivo in rows:
        if sid not in sentencias:
            sentencias[sid] = {
                "emb_text": emb_text,
                "fallo_macro": fallo_macro,
                "fallo_texto": fallo_texto or "",
                "texto_resolutivo": texto_resolutivo or "",
                "materias": set(),
                "tipo_proceso": tipo_proceso or "",
                "tribunal_id": tribunal_id,
            }
        if materia:
            sentencias[sid]["materias"].add(materia)
        if tipo_proceso and not sentencias[sid]["tipo_proceso"]:
            sentencias[sid]["tipo_proceso"] = tipo_proceso

    info(f"Sentencias únicas: {len(sentencias):,}")

    x_clasif_list: list[np.ndarray] = []
    x_emb_list: list[np.ndarray] = []
    y_list: list[str] = []
    materias_list: list[str] = []

    kw_names = keyword_feature_names()

    for data in sentencias.values():
        emb = np.array([float(x) for x in data["emb_text"].strip("[]").split(",")], dtype=np.float32)

        mat_oh = np.zeros(len(MATERIAS_CAT), dtype=np.float32)
        for idx, nombre in enumerate(MATERIAS_CAT):
            if nombre in data["materias"]:
                mat_oh[idx] = 1.0

        tp_oh = np.zeros(len(PROCESO_CAT), dtype=np.float32)
        tp = data["tipo_proceso"]
        for idx, categoria in enumerate(PROCESO_CAT):
            if categoria.lower() in tp.lower():
                tp_oh[idx] = 1.0
                break

        trib_oh = np.zeros(len(top_tribunales) + 1, dtype=np.float32)
        if data["tribunal_id"] in tribunal_index:
            trib_oh[tribunal_index[data["tribunal_id"]]] = 1.0
        else:
            trib_oh[-1] = 1.0

        kw_vec = build_keyword_vector(data["fallo_texto"], data["texto_resolutivo"])
        clasif_vec = np.concatenate([mat_oh * 3.0, tp_oh * 3.0, trib_oh * 2.0, kw_vec], dtype=np.float32)

        x_clasif_list.append(clasif_vec)
        x_emb_list.append(emb)
        y_list.append(data["fallo_macro"])
        materias_list.append(next(iter(data["materias"])) if data["materias"] else "Sin Materia")

    X_clasif = np.vstack(x_clasif_list).astype(np.float32)
    X_embeddings = np.vstack(x_emb_list).astype(np.float32)
    y = np.array(y_list)
    materias_arr = np.array(materias_list)

    info(f"Matriz clasificación: {X_clasif.shape[0]:,} × {X_clasif.shape[1]}")
    info(f"Embeddings para clustering: {X_embeddings.shape[0]:,} × {X_embeddings.shape[1]}")
    info(f"Features léxicas + metadatos: {len(kw_names) + len(MATERIAS_CAT) + len(PROCESO_CAT) + len(top_tribunales) + 1}")

    dist = Counter(y)
    print("\n  Distribución de clases (fallo_macro):")
    for clase, n in sorted(dist.items(), key=lambda item: -item[1]):
        bar = "█" * int(n / len(y) * 30)
        print(f"    {clase:<18} {n:>6,} ({n/len(y)*100:5.1f}%)  {bar}")

    return DatasetBundle(
        X_clasif=X_clasif,
        X_embeddings=X_embeddings,
        y=y,
        materias_arr=materias_arr,
        top_tribunales=top_tribunales,
        keyword_feature_names=kw_names,
    )


def entrenar_y_evaluar(bundle: DatasetBundle) -> dict:
    titulo("CLASIFICACIÓN — Base Learners + Stacking")

    le = LabelEncoder()
    y_enc = le.fit_transform(bundle.y)
    clases = le.classes_

    X_train, X_test, y_train, y_test = train_test_split(
        bundle.X_clasif,
        y_enc,
        test_size=0.20,
        random_state=RANDOM_STATE,
        stratify=y_enc,
    )
    info(f"Train: {len(X_train):,}  |  Test: {len(X_test):,}")

    modelos = {
        "Logistic(meta+kw)": Pipeline([
            ("scaler", StandardScaler()),
            ("lr", LogisticRegression(
                max_iter=2000,
                class_weight="balanced",
                solver="lbfgs",
                C=1.0,
                random_state=RANDOM_STATE,
            )),
        ]),
        "LinearSVC(meta+kw)": CalibratedClassifierCV(
            Pipeline([
                ("scaler", StandardScaler()),
                ("svc", LinearSVC(
                    class_weight="balanced",
                    C=0.9,
                    max_iter=10000,
                    random_state=RANDOM_STATE,
                )),
            ]),
            cv=3,
        ),
        "MultinomialNB(meta+kw)": MultinomialNB(alpha=0.2),
    }

    resultados = {}
    for nombre, modelo in modelos.items():
        sub(f"Entrenando {nombre}...")
        t0 = time.time()
        modelo.fit(X_train, y_train)
        elapsed = time.time() - t0
        pred = modelo.predict(X_test)
        acc = accuracy_score(y_test, pred) * 100
        f1m = f1_score(y_test, pred, average="macro") * 100
        f1w = f1_score(y_test, pred, average="weighted") * 100
        info(f"Tiempo entrenamiento: {elapsed:.1f}s")
        print(f"\n  {BOLD}{nombre}:{X}")
        print(f"    Accuracy   : {G if acc >= 88 else Y}{acc:.2f}%{X}")
        print(f"    F1-macro   : {G if f1m >= 80 else Y}{f1m:.2f}%{X}")
        print(f"    F1-weighted: {G if f1w >= 85 else Y}{f1w:.2f}%{X}")
        resultados[nombre] = {
            "modelo": modelo,
            "acc": acc,
            "f1_macro": f1m,
            "f1_weighted": f1w,
            "pred": pred,
        }

    sub("Entrenando StackingClassifier...")
    stacking = StackingClassifier(
        estimators=[
            ("lr_meta_kw", clone(modelos["Logistic(meta+kw)"])),
            ("svc_meta_kw", clone(modelos["LinearSVC(meta+kw)"])),
            ("nb_meta_kw", clone(modelos["MultinomialNB(meta+kw)"])),
        ],
        final_estimator=LogisticRegression(
            max_iter=1200,
            class_weight="balanced",
            solver="lbfgs",
            C=1.0,
            random_state=RANDOM_STATE,
        ),
        stack_method="predict_proba",
        cv=STACKING_CV,
        n_jobs=1,
    )
    t0 = time.time()
    stacking.fit(X_train, y_train)
    elapsed = time.time() - t0
    pred = stacking.predict(X_test)
    acc = accuracy_score(y_test, pred) * 100
    f1m = f1_score(y_test, pred, average="macro") * 100
    f1w = f1_score(y_test, pred, average="weighted") * 100
    info(f"Tiempo entrenamiento stack: {elapsed:.1f}s")
    print(f"\n  {BOLD}StackingClassifier:{X}")
    print(f"    Accuracy   : {G if acc >= 88 else Y}{acc:.2f}%{X}")
    print(f"    F1-macro   : {G if f1m >= 80 else Y}{f1m:.2f}%{X}")
    print(f"    F1-weighted: {G if f1w >= 85 else Y}{f1w:.2f}%{X}")
    resultados["StackingClassifier"] = {
        "modelo": stacking,
        "acc": acc,
        "f1_macro": f1m,
        "f1_weighted": f1w,
        "pred": pred,
    }

    mejor_nombre = max(resultados, key=lambda nombre: resultados[nombre]["acc"])
    mejor = resultados[mejor_nombre]
    sub(f"Mejor modelo: {mejor_nombre} (Accuracy: {mejor['acc']:.2f}%)")

    print(f"\n{BOLD}  Reporte por clase ({mejor_nombre}):{X}")
    reporte = classification_report(y_test, mejor["pred"], target_names=clases, digits=3)
    for linea in reporte.split("\n"):
        print(f"    {linea}")

    sub("Chequeo de clases delicadas...")
    precision, recall, f1_cls, support = precision_recall_fscore_support(
        y_test,
        mejor["pred"],
        labels=np.arange(len(clases)),
        zero_division=0,
    )
    for idx, clase in enumerate(clases):
        if clase in {"CASACION", "NULIDAD", "SOBRESEIMIENTO", "OTRO"}:
            print(
                f"    {clase:<18} "
                f"precision={precision[idx]:.3f} "
                f"recall={recall[idx]:.3f} "
                f"f1={f1_cls[idx]:.3f} "
                f"support={support[idx]}"
            )

    sub("Validación cruzada del mejor modelo...")
    cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
    scores_cv = cross_val_score(mejor["modelo"], bundle.X_clasif, y_enc, cv=cv, scoring="accuracy", n_jobs=1)
    print(f"\n  CV Accuracy: {scores_cv.mean()*100:.2f}% ± {scores_cv.std()*100:.2f}%")
    print(f"  Folds: {[f'{score*100:.2f}%' for score in scores_cv]}")

    return {
        "label_encoder": le,
        "clases": clases,
        "mejor_nombre": mejor_nombre,
        "mejor_modelo": mejor["modelo"],
        "mejor_accuracy": mejor["acc"],
        "mejor_f1_macro": mejor["f1_macro"],
        "cv_accuracy": scores_cv.mean() * 100,
        "cv_std": scores_cv.std() * 100,
    }


def guardar_modelo(bundle: DatasetBundle, resultados: dict) -> str:
    sub("Guardando modelo entrenado...")
    artefacto = {
        "modelo": resultados["mejor_modelo"],
        "label_encoder": resultados["label_encoder"],
        "materias_cat": MATERIAS_CAT,
        "proceso_cat": PROCESO_CAT,
        "top_tribunales": bundle.top_tribunales,
        "keyword_feature_names": bundle.keyword_feature_names,
        "keyword_specs": KEYWORD_SPECS,
        "rule_signal_specs": RULE_SIGNAL_SPECS,
        "classification_dim": int(bundle.X_clasif.shape[1]),
        "accuracy": resultados["mejor_accuracy"],
        "f1_macro": resultados["mejor_f1_macro"],
        "cv_accuracy": resultados["cv_accuracy"],
        "cv_std": resultados["cv_std"],
        "nombre": resultados["mejor_nombre"],
        "uses_fallo_text": True,
    }
    ruta_modelo = os.path.join(MODELOS_DIR, "clasificador_fallos.pkl")
    with open(ruta_modelo, "wb") as archivo:
        pickle.dump(artefacto, archivo)
    ok(f"Modelo guardado en: {ruta_modelo}")
    return ruta_modelo


def evaluar_clustering(bundle: DatasetBundle) -> None:
    titulo("CLUSTERING — Referencia rápida")
    y_mat = LabelEncoder().fit_transform(bundle.materias_arr)
    resultados = []

    for k in [6, 8, 10]:
        sub(f"MiniBatchKMeans K={k}...")
        t0 = time.time()
        km = MiniBatchKMeans(
            n_clusters=k,
            random_state=RANDOM_STATE,
            batch_size=1024,
            n_init=5,
            max_iter=100,
        )
        labels = km.fit_predict(bundle.X_embeddings)
        elapsed = time.time() - t0
        sample_idx = np.random.choice(len(bundle.X_embeddings), min(3000, len(bundle.X_embeddings)), replace=False)
        sil = silhouette_score(bundle.X_embeddings[sample_idx], labels[sample_idx], metric="cosine")
        ari = adjusted_rand_score(y_mat, labels)
        resultados.append((k, sil, ari, elapsed))
        print(f"    K={k:<2}  Silhouette={sil:.4f}  ARI={ari:.4f}  Tiempo={elapsed:.1f}s")

    mejor = max(resultados, key=lambda item: item[1])
    print(f"\n  Mejor K por silhouette: K={mejor[0]}  Sil={mejor[1]:.4f}  ARI={mejor[2]:.4f}")


def resumen_final(ruta_modelo: str, resultados: dict) -> None:
    titulo("RESUMEN FINAL")
    print(f"  Mejor modelo       : {BOLD}{resultados['mejor_nombre']}{X}")
    print(f"  Accuracy test      : {BOLD}{resultados['mejor_accuracy']:.2f}%{X}")
    print(f"  CV accuracy        : {BOLD}{resultados['cv_accuracy']:.2f}% ± {resultados['cv_std']:.2f}%{X}")
    print(f"  Artefacto          : {ruta_modelo}")

    if resultados["mejor_accuracy"] >= 88:
        ok("Objetivo alcanzado: accuracy >= 88%.")
    else:
        warn("Aún no llega a 88%; revisa vocabulario jurídico o reduce dependencia de clases raras.")


def main() -> None:
    bundle = cargar_dataset()
    resultados = entrenar_y_evaluar(bundle)
    ruta_modelo = guardar_modelo(bundle, resultados)
    evaluar_clustering(bundle)
    resumen_final(ruta_modelo, resultados)


if __name__ == "__main__":
    main()
