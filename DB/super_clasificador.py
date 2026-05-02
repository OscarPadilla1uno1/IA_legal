import sys, os, time, pickle
sys.stdout.reconfigure(encoding="utf-8")
import numpy as np
import psycopg2
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import classification_report, accuracy_score, f1_score
from sklearn.preprocessing import LabelEncoder

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

def run_super_test():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    
    print("▶ Cargando datos enriquecidos...")
    cur.execute("""
        SELECT s.embedding::text, s.fallo_macro, m.nombre, tp.nombre, s.tribunal_id::text
        FROM sentencias s
        LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
        LEFT JOIN materias m ON m.id = sm.materia_id
        LEFT JOIN tipos_proceso tp ON tp.id = s.tipo_proceso_id
        WHERE s.embedding IS NOT NULL AND s.fallo_macro IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    X, y = [], []
    MAT_CAT = ['Derecho Civil', 'Derecho Penal', 'Derecho Laboral', 'Contencioso Administrativo', 'Derecho Constitucional']
    
    for r in rows:
        emb = np.array([float(x) for x in r[0].strip("[]").split(",")], dtype=np.float32)
        # Metadata OH
        m_oh = [1.0 if r[2] == c else 0.0 for c in MAT_CAT]
        feat = np.concatenate([emb, m_oh])
        X.append(feat)
        y.append(r[1])

    X = np.array(X)
    le = LabelEncoder()
    y_enc = le.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(X, y_enc, test_size=0.2, random_state=42, stratify=y_enc)

    print(f"🚀 Entrenando RandomForest con {len(X_train)} ejemplos...")
    rf = RandomForestClassifier(n_estimators=200, class_weight="balanced", n_jobs=-1, random_state=42)
    rf.fit(X_train, y_train)

    y_pred = rf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average='macro')

    print(f"\n✅ RESULTADO:")
    print(f"   Accuracy: {acc*100:.2f}%")
    print(f"   F1-Macro: {f1*100:.2f}%")
    print("\nDetalle por clase:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

if __name__ == "__main__":
    run_super_test()
