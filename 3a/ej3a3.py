"""
Enunciado:
En este ejercicio aprenderás a trabajar con bases de datos SQLite existentes.
Aprenderás a:
1. Conectar a una base de datos SQLite existente
2. Convertir datos de SQLite a formatos compatibles con JSON
3. Extraer datos de SQLite a pandas DataFrame

El archivo ventas_comerciales.db contiene datos de ventas con tablas relacionadas
que incluyen productos, vendedores, regiones y ventas. Debes analizar estos datos
usando diferentes técnicas.
"""

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "ventas_comerciales.db")


def conectar_bd() -> sqlite3.Connection:
    """
    Conecta a una base de datos SQLite existente

    Returns:
        sqlite3.Connection: Objeto de conexión a la base de datos SQLite
    """
    # Implementa aquí la conexión a la base de datos:
    # 1. Verifica que el archivo de base de datos existe
    # 2. Conecta a la base de datos
    # 3. Configura la conexión para que devuelva las filas como diccionarios (opcional)
    # 4. Retorna la conexión

    try:
        # 1. Verificar que el archivo de base de datos existe (o intentar conectar de todas formas)
        # if not os.path.exists(DB_PATH):
        #     raise FileNotFoundError(f"No se encontró el archivo de base de datos: {DB_PATH}")

        # 2. Conecta a la base de datos (se creará si no existe)
        conexion = sqlite3.connect(DB_PATH)

        # 3. Configurar la conexión para que devuelva las filas como diccionarios
        conexion.row_factory = sqlite3.Row

        # 4. Retorna la conexión
        return conexion

    except sqlite3.Error as e:
        print(f"Error al conectar con la base de datos: {e}")
        raise


def convertir_a_json(conexion: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convierte los datos de la base de datos en un objeto compatible con JSON

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, List[Dict[str, Any]]]: Diccionario con todas las tablas y sus registros
        en formato JSON-serializable
    """
    # Implementa aquí la conversión de datos a formato JSON:
    # 1. Crea un diccionario vacío para almacenar el resultado
    # 2. Obtén la lista de tablas de la base de datos
    # 3. Para cada tabla:
    #    a. Ejecuta una consulta SELECT * FROM tabla
    #    b. Obtén los nombres de las columnas
    #    c. Convierte cada fila a un diccionario (clave: nombre columna, valor: valor celda)
    #    d. Añade el diccionario a una lista para esa tabla
    # 4. Retorna el diccionario completo con todas las tablas

    try:
        # 1. Crear un diccionario vacío para almacenar el resultado
        datos_json = {}

        # 2. Obtener la lista de tablas de la base de datos
        cursor = conexion.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        )
        tablas = [tabla[0] for tabla in cursor.fetchall()]

        # 3. Para cada tabla:
        for tabla in tablas:
            # 3a. Ejecutar una consulta SELECT * FROM tabla
            cursor.execute(f"SELECT * FROM {tabla}")
            filas = cursor.fetchall()

            # 3b. Obtener los nombres de las columnas
            columnas = [descripcion[0] for descripcion in cursor.description]

            # 3c. Convertir cada fila a un diccionario
            registros = []
            for fila in filas:
                registro = {}
                for i, columna in enumerate(columnas):
                    registro[columna] = fila[i]
                registros.append(registro)

            # 3d. Añadir el diccionario a una lista para esa tabla
            datos_json[tabla] = registros

        # 4. Retornar el diccionario completo con todas las tablas
        return datos_json

    except sqlite3.Error as e:
        print(f"Error al convertir datos a JSON: {e}")
        return {}


def convertir_a_dataframes(conexion: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """
    Extrae los datos de la base de datos a DataFrames de pandas

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, pd.DataFrame]: Diccionario con DataFrames para cada tabla y para
        consultas combinadas relevantes
    """
    # Implementa aquí la extracción de datos a DataFrames:
    # 1. Crea un diccionario vacío para los DataFrames
    # 2. Obtén la lista de tablas de la base de datos
    # 3. Para cada tabla, crea un DataFrame usando pd.read_sql_query
    # 4. Añade consultas JOIN para relaciones importantes:
    #    - Ventas con información de productos
    #    - Ventas con información de vendedores
    #    - Vendedores con regiones
    # 5. Retorna el diccionario con todos los DataFrames

    try:
        # 1. Crear diccionario vacío para los DataFrames
        dataframes = {}

        # 2. Obtener lista de tablas de la base de datos
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        nombres_tablas = [tabla[0] for tabla in tablas]

        # 3. Para cada tabla, crear DataFrame usando pd.read_sql_query
        for nombre_tabla in nombres_tablas:
            query = f"SELECT * FROM {nombre_tabla}"
            df = pd.read_sql_query(query, conexion)
            dataframes[nombre_tabla] = df

        # 4. Añadir consultas JOIN para relaciones importantes:

        # - Ventas con información de productos
        query_ventas_productos = """
            SELECT v.*, p.nombre as producto_nombre, p.categoria, p.precio_unitario
            FROM ventas v
            JOIN productos p ON v.producto_id = p.id
        """
        df_ventas_productos = pd.read_sql_query(query_ventas_productos, conexion)
        dataframes["ventas_productos"] = df_ventas_productos

        # - Ventas con información de vendedores
        query_ventas_vendedores = """
            SELECT v.*, vd.nombre as vendedor_nombre, r.nombre as region_nombre, r.pais
            FROM ventas v
            JOIN vendedores vd ON v.vendedor_id = vd.id
            JOIN regiones r ON vd.region_id = r.id
        """
        df_ventas_vendedores = pd.read_sql_query(query_ventas_vendedores, conexion)
        dataframes["ventas_vendedores"] = df_ventas_vendedores

        # - Vendedores con regiones
        query_vendedores_regiones = """
            SELECT vd.*, r.nombre as region_nombre, r.pais
            FROM vendedores vd
            JOIN regiones r ON vd.region_id = r.id
        """
        df_vendedores_regiones = pd.read_sql_query(query_vendedores_regiones, conexion)
        dataframes["vendedores_regiones"] = df_vendedores_regiones

        # 5. Retornar el diccionario con todos los DataFrames
        return dataframes

    except Exception as e:
        print(f"Error al convertir datos a DataFrames: {e}")
        return {}


if __name__ == "__main__":
    try:
        # Conectar a la base de datos existente
        print("Conectando a la base de datos...")
        conexion = conectar_bd()
        print("Conexión establecida correctamente.")

        # Verificar la conexión mostrando las tablas disponibles
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        print(f"\nTablas en la base de datos: {[t[0] for t in tablas]}")

        # Conversión a JSON
        print("\n--- Convertir datos a formato JSON ---")
        datos_json = convertir_a_json(conexion)
        print("Estructura JSON (ejemplo de una tabla):")
        if datos_json:
            # Muestra un ejemplo de la primera tabla encontrada
            primera_tabla = list(datos_json.keys())[0]
            print(f"Tabla: {primera_tabla}")
            if datos_json[primera_tabla]:
                print(f"Primer registro: {datos_json[primera_tabla][0]}")

            # Opcional: guardar los datos en un archivo JSON
            # ruta_json = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.json')
            # with open(ruta_json, 'w', encoding='utf-8') as f:
            #     json.dump(datos_json, f, ensure_ascii=False, indent=2)
            # print(f"Datos guardados en {ruta_json}")

        # Conversión a DataFrames de pandas
        print("\n--- Convertir datos a DataFrames de pandas ---")
        dataframes = convertir_a_dataframes(conexion)
        if dataframes:
            print(f"Se han creado {len(dataframes)} DataFrames:")
            for nombre, df in dataframes.items():
                print(f"- {nombre}: {len(df)} filas x {len(df.columns)} columnas")
                print(f"  Columnas: {', '.join(df.columns.tolist())}")
                print(f"  Vista previa:\n{df.head(2)}\n")

    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "conexion" in locals() and conexion:
            conexion.close()
            print("\nConexión cerrada.")
