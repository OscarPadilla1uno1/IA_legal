import os
import fitz
from pathlib import Path

def crear_codigo_dummy():
    path = Path(r"D:\CodigosHN_v2\Codigo_Penal_Dummy.pdf")
    path.parent.mkdir(parents=True, exist_ok=True)
    
    doc = fitz.open()
    page = doc.new_page()
    texto = """CÓDIGO PENAL DE HONDURAS

LIBRO PRIMERO: Disposiciones Generales
TÍTULO I: De la Ley Penal

CAPÍTULO I
Garantías Penales y de la Aplicación de la Ley Penal

ARTÍCULO 1.- Principio de Legalidad.
Nadie podrá ser castigado por acción u omisión que no esté expresamente prevista como delito o falta por Ley vigente al tiempo de su perpetración.

ARTÍCULO 2.- Principio de Lesividad.
Ninguna acción u omisión será castigada si no lesiona o pone en peligro un bien jurídico tutelado por la Ley Penal.

LIBRO SEGUNDO
TÍTULO II
CAPÍTULO II
De Las Penas

ARTÍCULO 3.- Clases de Penas.
Las penas que pueden imponerse con arreglo a este Código, bien con carácter principal, bien como accesorias, son privativas de libertad, privativas de otros derechos y multa.
"""
    page.insert_text((50, 50), texto, fontsize=12)
    doc.save(str(path))
    doc.close()
    print(f"Creado: {path}")

def crear_gaceta_dummy():
    path = Path(r"D:\GacetasHN\La_Gaceta_35800.pdf")
    path.parent.mkdir(parents=True, exist_ok=True)
    
    doc = fitz.open()
    page = doc.new_page()
    texto = """LA GACETA
DIARIO OFICIAL DE LA REPÚBLICA DE HONDURAS
DECANO DE LA PRENSA HONDUREÑA
TEGUCIGALPA, M. D. C., 10 DE SEPTIEMBRE DE 2023 - EDICIÓN 35800

ACUERDO EJECUTIVO NÚMERO 04-2023
LA PRESIDENTA DE LA REPÚBLICA

CONSIDERANDO: Que la Constitución de la República establece que el Estado tiene la obligación indeclinable...
CONSIDERANDO: Que el acceso a la vivienda es un derecho humano...

POR TANTO: En el ejercicio de las facultades que le confiere la Ley...

ACUERDA:
PRIMERO: Crear el "Bono Solidario de Vivienda" para familias de escasos recursos.
SEGUNDO: Instruir a la Secretaría de Finanzas asignar presupuestos inmediatos.
"""
    page.insert_text((50, 50), texto, fontsize=12)
    doc.save(str(path))
    doc.close()
    print(f"Creado: {path}")

if __name__ == "__main__":
    crear_codigo_dummy()
    crear_gaceta_dummy()
