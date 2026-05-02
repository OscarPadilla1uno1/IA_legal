import psycopg2
import sys
import time

DB_PARAMS = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

def auditoria_ultra():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    t_inicio = time.time()
    
    print("\n" + "█"*70)
    print("      ESCÁNER FORENSE DE BASE DE DATOS - IA LEGAL v2.0")
    print("█"*70)

    # 1. VOLUMEN E INTEGRIDAD
    print(f"\n[1] ESTRUCTURA GENERAL")
    cur.execute("SELECT COUNT(*) FROM sentencias")
    ts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fragmentos_texto")
    tf = cur.fetchone()[0]
    print(f"    - Sentencias totales: {ts:,}")
    print(f"    - Fragmentos totales: {tf:,}")

    # 2. DETECCIÓN DE DUPLICADOS (Aprox. por Hash)
    print(f"\n[2] DETECCIÓN DE REDUNDANCIA")
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT texto_integro, COUNT(*) 
            FROM sentencias 
            WHERE texto_integro IS NOT NULL 
            GROUP BY texto_integro HAVING COUNT(*) > 1
        ) AS dups
    """)
    dups = cur.fetchone()[0]
    print(f"    - Grupos de sentencias 100% duplicadas: {dups:,}")

    # 3. SALUD VECTORIAL (BGE-M3)
    print(f"\n[3] INTEGRIDAD SEMÁNTICA (VECTORES)")
    cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NULL")
    f_sin_v = cur.fetchone()[0]
    # Buscar vectores con magnitud cero (anomalas usando 1024 dims)
    cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento = array_fill(0, ARRAY[1024])::vector")
    f_zero = cur.fetchone()[0]
    
    print(f"    - Cobertura: {((tf - f_sin_v)/tf)*100:.2f}%")
    print(f"    - Fragmentos sin procesar: {f_sin_v:,}")
    print(f"    - Vectores corruptos (magnitud 0): {f_zero:,}")

    # 4. CALIDAD DE METADATOS (MÍNIMO VIABLE PARA ML)
    print(f"\n[4] CONSISTENCIA DE METADATOS")
    cur.execute("SELECT COUNT(*) FROM sentencias WHERE fallo_macro IS NULL")
    s_f = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sentencias WHERE fecha_resolucion IS NULL")
    s_d = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sentencias s WHERE NOT EXISTS (SELECT 1 FROM sentencias_materias sm WHERE sm.sentencia_id = s.id)")
    s_m = cur.fetchone()[0]

    print(f"    - Sentencias sin etiqueta de fallo: {s_f:,}")
    print(f"    - Sentencias sin fecha valida: {s_d:,}")
    print(f"    - Sentencias sin materia asignada: {s_m:,}")

    # 5. ANOMALÍAS TEMPORALES
    print(f"\n[5] LÍNEA DE TIEMPO")
    cur.execute("SELECT MIN(fecha_resolucion), MAX(fecha_resolucion) FROM sentencias")
    f_min, f_max = cur.fetchone()
    print(f"    - Sentencia mas antigua: {f_min}")
    print(f"    - Sentencia mas reciente: {f_max}")
    if f_max and f_max.year > 2026:
        print(f"    - ¡ALERTA!: Hay fechas futuras en la base de datos.")

    # 6. LIMPIEZA DE TEXTO (BASURA Y RUIDO)
    print(f"\n[6] CALIDAD DEL CONTENIDO")
    cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE contenido ~ '^[[:punct:][:space:]]+$'")
    basura = cur.fetchone()[0]
    print(f"    - Fragmentos que son solo simbolos/espacios: {basura:,}")

    # 7. TABLAS RELACIONALES (HUÉRFANOS)
    print(f"\n[7] VERIFICACIÓN DE VÍNCULOS (N:M)")
    cur.execute("SELECT COUNT(*) FROM sentencias_materias WHERE sentencia_id NOT IN (SELECT id FROM sentencias)")
    h_m = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sentencias_articulos_ley WHERE sentencia_id NOT IN (SELECT id FROM sentencias)")
    h_a = cur.fetchone()[0]
    print(f"    - Vinculos de materia huerfanos: {h_m}")
    print(f"    - Vinculos de articulos huerfanos: {h_a}")

    print("\n" + "█"*70)
    print(f"      TIEMPO TOTAL DE ESCANEO: {time.time()-t_inicio:.2f}s")
    print("█"*70 + "\n")
    conn.close()

if __name__ == "__main__":
    auditoria_ultra()
