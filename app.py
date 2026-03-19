import streamlit as st
import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(".env_prod")

# 1. Configuración de la conexión
def crear_conexion():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "mis-vecinos")
    )

st.title("📊 Extractor de Datos Históricos")

# 2. Interfaz: Selector de rango de fechas
col1, col2 = st.columns(2)
with col1:
    fecha_inicio = st.date_input("Fecha inicial")
with col2:
    fecha_fin = st.date_input("Fecha final")

if st.button("Consultar Datos"):
    try:
        conn = crear_conexion()
        # 3. Consulta SQL con parámetros para evitar inyecciones
        query = """
            SELECT
                ar.created_at AS fecha,
                CONCAT_WS(' ',
                    NULLIF(h.street, ''),
                    CONCAT('Mz. ', NULLIF(h.mz, '')),
                    NULLIF(h.inner_number, '')
                ) AS casa,
                a.name AS visitante,
                COALESCE(v.name, CONCAT(u.name, ' ', IFNULL(u.last_name, ''))) AS vigilante,
                ar.type AS tipo
            FROM access_record ar
            INNER JOIN accesses a ON a.id = ar.access_id
            INNER JOIN houses h ON h.id = a.house_id
            LEFT JOIN vigilances v ON v.user_id = ar.vigilant_id
            LEFT JOIN users u ON u.id = ar.vigilant_id
            WHERE h.housing_id = 15
              AND ar.created_at BETWEEN %s AND %s
            ORDER BY ar.created_at DESC;
        """
        
        # Ajustamos las fechas para que abarquen desde las 00:00:00 hasta las 23:59:59
        fecha_ini_str = f"{fecha_inicio} 00:00:00"
        fecha_fin_str = f"{fecha_fin} 23:59:59"

        # Ejecutar y cargar en un DataFrame de Pandas
        df = pd.read_sql(query, conn, params=(fecha_ini_str, fecha_fin_str))
        conn.close()

        if not df.empty:
            st.success(f"Se encontraron {len(df)} registros.")
            st.dataframe(df) # Muestra una vista previa en la web

            # 4. Botón para descargar CSV
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Descargar datos como CSV",
                data=csv,
                file_name=f"datos_{fecha_inicio}_al_{fecha_fin}.csv",
                mime="text/csv",
            )
        else:
            st.warning("No hay datos para el rango seleccionado.")
            
    except Exception as e:
        st.error(f"Error de conexión: {e}")