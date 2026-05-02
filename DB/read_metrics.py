import pickle
with open('DB/modelos_ml/clasificador_fallos.pkl', 'rb') as f:
    data = pickle.load(f)
    acc = data.get('accuracy', 0)
    f1 = data.get('f1_macro', 0)
    cv = data.get('cv_accuracy', 0)
    print(f"Accuracy: {acc*100:.2f}%")
    print(f"F1-Macro: {f1*100:.2f}%")
    print(f"CV Accuracy: {cv*100:.2f}%")
