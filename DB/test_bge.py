import requests
import numpy as np

def get_ollama_embeddings(texts):
    OLLAMA_URL = "http://localhost:11434/api/embed"
    payload = {
        "model": "bge-m3",
        "input": texts
    }
    response = requests.post(OLLAMA_URL, json=payload)
    return response.json().get("embeddings", [])

v = get_ollama_embeddings([
    "es inadmisible el recurso de amparo cuando se aleguen cuestiones de mera legalidad",
    "es inadmisible el recurso de amparo cuando se aleguen violaciones de mera legalidad"
])

def cosine_sim(a, b):
    return np.dot(a, b)/(np.linalg.norm(a)*np.linalg.norm(b))

print("Similitud:", cosine_sim(v[0], v[1]))
