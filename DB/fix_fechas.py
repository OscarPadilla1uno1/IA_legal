import sys
sys.stdout.reconfigure(encoding='utf-8')
import re
import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Traer todas las sentencias sin fecha
    cursor.execute("SELECT id, numero_sentencia FROM sentencias WHERE fecha_resolucion IS NULL;")
    rows = cursor.fetchall()
    
    print(f"Sentencias sin fecha: {len(rows)}")
    
    # Patrón: FRes_DD_MM_YYYY
    patron = re.compile(r'FRes_(\d{2})_(\d{2})_(\d{4})')
    
    actualizadas = 0
    sin_match = 0
    
    for sid, nombre in rows:
        match = patron.search(nombre)
        if match:
            dia, mes, anio = match.groups()
            fecha = f"{anio}-{mes}-{dia}"  # formato ISO: YYYY-MM-DD
            cursor.execute(
                "UPDATE sentencias SET fecha_resolucion = %s WHERE id = %s;",
                (fecha, sid)
            )
            actualizadas += 1
        else:
            sin_match += 1
            if sin_match <= 5:
                print(f"  Sin patrón FRes: {nombre}")
    
    conn.commit()
    
    # Verificar resultado
    cursor.execute("SELECT COUNT(*) FROM sentencias WHERE fecha_resolucion IS NULL;")
    restantes = cursor.fetchone()[0]
    
    print(f"\n✅ Fechas actualizadas: {actualizadas}")
    print(f"⚠️  Sin patrón reconocible: {sin_match}")
    print(f"📊 Sentencias aún sin fecha: {restantes}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
