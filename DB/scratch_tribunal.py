import re

text = """Tribunal de procedencia
Corte de Apelaciones del Trabajo de San Pedro Sula, Cortés
Fecha de sentencia recurrida"""

pattern = r"Tribunal(?: de procedencia)?:?[\s]*[\r\n]+(.*?)(?=\nFecha de sentencia recurrida)"
match = re.search(pattern, text, flags=re.DOTALL | re.IGNORECASE)
if match:
    print(f"Match: {match.group(1).strip()}")
else:
    print("No match")

# Try a more liberal one
pattern2 = r"Tribunal(?: de procedencia)?:?[\s]*[\r\n]+(.*?)(?=\nFecha)"
match2 = re.search(pattern2, text, flags=re.DOTALL | re.IGNORECASE)
if match2:
    print(f"Match2: {match2.group(1).strip()}")
