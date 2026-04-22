import requests
import numpy as np

def get_ollama_embeddings(texts):
    OLLAMA_URL = "http://localhost:11434/api/embed"
    payload = {"model": "bge-m3", "input": texts}
    response = requests.post(OLLAMA_URL, json=payload)
    return response.json().get("embeddings", [])

def cosine_sim(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b)/(np.linalg.norm(a)*np.linalg.norm(b))

# El chunk real que está en tu BD
chunk = "el artículo 46 numeral 1) de la ley Sobre Justicia Constitucional dispone, que es inadmisible el recurso de amparo, cuando se aleguen violaciones de mera legalidad."

# Tu consulta
consulta = "es inadmisible el recurso de amparo cuando se aleguen cuestiones de mera legalidad"

# Prueba 1 — sin prefijos (lo que tienes ahora)
print("Generando vectores a través de Ollama (Sin Prefijo)...")
v_sin = get_ollama_embeddings([chunk, consulta])
sin_prefijo = cosine_sim(v_sin[0], v_sin[1])

# Prueba 2 — con prefijos correctos
print("Generando vectores a través de Ollama (Con Prefijo)...")
v_con = get_ollama_embeddings([f"passage: {chunk}", f"query: {consulta}"])
con_prefijo = cosine_sim(v_con[0], v_con[1])

print("---------------------------------")
print(f"Sin prefijo: {sin_prefijo:.4f}")
print(f"Con prefijo: {con_prefijo:.4f}")
print(f"Diferencia:  {(con_prefijo - sin_prefijo):.4f}")
