import re

text = """Sentencia  CL-795 -00
Tipo de proceso
Casación
Sub tipo de proceso
--
Fecha de resolución
01-09-2000
Magistrado ponente
Teodolinda Pineda Cardona
Materia
Derecho Laboral
Recurrente
Carlos Mario Villalobos Manzanares, Fredy David Turcios Gómez,
Dennis Israel Reconco Navarro, y Erin Bladimir Romero Tabora
Recurrido
Empresa de Mantenimiento, Construcción Y Electricidad Sociedad
Anónima De Capital Variable (EMCE S. A. de C. V.),
Tribunal de procedencia
Corte de Apelaciones del Trabajo de San Pedro Sula, Cortés
Fecha de sentencia recurrida
29-03-2000
Motivo de la casación
Hechos relevantes
El recurrente alega que en primera instancia se declaro sin lugar la
demanda ordinaria laboral...
Anonimizada
No
Fallo
No ha lugar
Tesauro
Derecho Procesal Laboral"""

patterns = {
    "sentencia_id": r"Sentencia\s+([^\r\n]+)",
    "tipo_proceso": r"Tipo de proceso:?[\s]*[\r\n]+([^\r\n]+)",
    "subtipo_proceso": r"Sub ?tipo de proceso:?[\s]*[\r\n]+([^\r\n]+)",
    "magistrado": r"Magistrado ponente:?[\s]*[\r\n]+([^\r\n]+)",
    "materia": r"Materia:?[\s]*[\r\n]+([^\r\n]+)",
    "fecha_resolucion": r"Fecha de resolución:?[\s]*[\r\n]+([\d/\-]+)",
    "recurrente": r"Recurrente:?[\s]*[\r\n]+(.*?)(?=\nRecurrido:?|\nTribunal)",
    "recurrido": r"Recurrido:?[\s]*[\r\n]+(.*?)(?=\nTribunal)",
    "tribunal": r"Tribunal(?: de procedencia)?:?[\s]*[\r\n]+(.*?)(?=\nFecha de sentencia recurrida)",
    "fecha_sentencia_recurrida": r"Fecha de sentencia[\s]*[\r\n]*recurrida:?[\s]*[\r\n]+([\d/\-]+)",
    "fallo": r"Fallo:?[\s]*[\r\n]+(.+?(?=\nHechos relevantes:?|\nTesauro:?|\Z))"
}

for key, pattern in patterns.items():
    match = re.search(pattern, text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        val = match.group(1).replace('\n', ' ').strip()
        print(f"{key}: {val}")
    else:
        print(f"{key}: NONE")
