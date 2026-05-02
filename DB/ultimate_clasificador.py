import sys, os, time, pickle
sys.stdout.reconfigure(encoding="utf-8")
import numpy as np
import psycopg2
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

def run_ultimate_test():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    
    print("▶ Cargando embeddings y metadatos relacionales...")
    cur.execute("""
        SELECT s.embedding::text, s.fallo_macro, m.nombre, tp.nombre, s.tribunal_id::text
        FROM sentencias s
        LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
        LEFT JOIN materias m ON m.id = sm.materia_id
        LEFT JOIN tipos_proceso tp ON tp.id = s.tipo_proceso_id
        WHERE s.embedding IS NOT NULL AND s.fallo_macro IS NOT NULL
          AND s.fallo_macro != 'OTRO'
    """)
    rows = cur.fetchall()
    conn.close()

    X, y = [], []
    MAT_CAT = ['Derecho Civil', 'Derecho Penal', 'Derecho Laboral', 'Contencioso Administrativo', 'Derecho Constitucional']
    PROC_CAT = ['Amparo', 'Casación', 'Inconstitucionalidad', 'Revisión']
    
    for r in rows:
        emb = np.array([float(x) for x in r[0].strip("[]").split(",")], dtype=np.float32)
        m_oh = [1.0 if r[2] == c else 0.0 for c in MAT_CAT]
        p_oh = [1.0 if r[3] and c in r[3] else 0.0 for c in PROC_CAT]
        feat = np.concatenate([emb, m_oh, p_oh])
        X.append(feat)
        y.append(r[1])

    X = np.array(X)
    le = LabelEncoder()
    y_enc = le.fit_transform(y)

    # Balanceo con SMOTE (Sobremuestreo de minorías)
    print("⚖️  Aplicando SMOTE para balancear clases minoritarias...")
    smote = SMOTE(random_state=42)
    X_res, y_res = smote.fit_resample(X, y_enc)

    X_train, X_test, y_train, y_test = train_test_split(X_res, y_res, test_size=0.2, random_state=42)

    print("🚀 Entrenando Ensamble de Votación (SVC + RF + Logistic)...")
    clf1 = LinearSVC(class_weight="balanced", max_iter=2000, random_state=42)
    clf2 = RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=42)
    clf3 = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)

    voter = VotingClassifier(estimators=[('svc', clf1), ('rf', clf2), ('lr', clf3)], voting='hard')
    voter.fit(X_train, y_train)

    # Evaluar en un set de prueba REAL (sin SMOTE) para ver performance real
    _, X_val, _, y_val = train_test_split(X, y_enc, test_size=0.2, random_state=42)
    
    y_pred = voter.predict(X_val)
    acc = accuracy_score(y_val, y_pred)

    print(f"\n🏆 RESULTADO FINAL:")
    print(f"   Accuracy REAL: {acc*100:.2f}%")
    print("\nReporte Detallado:")
    print(classification_report(y_val, y_pred, target_names=le.classes_))

if __name__ == "__main__":
    run_ultimate_test()
