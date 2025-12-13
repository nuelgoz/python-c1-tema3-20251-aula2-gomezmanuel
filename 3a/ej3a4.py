"""
Enunciado:
En este ejercicio aprenderás a utilizar MongoDB con Python para trabajar
con bases de datos NoSQL. MongoDB es una base de datos orientada a documentos que
almacena datos en formato similar a JSON (BSON).

Tareas:
1. Conectar a una base de datos MongoDB
2. Crear colecciones (equivalentes a tablas en SQL)
3. Insertar, actualizar, consultar y eliminar documentos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de MongoDB desde Python utilizando PyMongo.
"""

import os
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pymongo
from bson.objectid import ObjectId

# Configuración de MongoDB
DB_NAME = "biblioteca"
MONGODB_PORT = 27017


def verificar_mongodb_instalado() -> bool:
    """
    Verifica si MongoDB está instalado en el sistema
    """
    try:
        # Intentamos ejecutar mongod --version para verificar que está instalado
        result = subprocess.run(
            ["mongod", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def iniciar_mongodb_en_memoria() -> Optional[subprocess.Popen]:
    """
    Inicia una instancia de MongoDB en memoria para pruebas
    """
    # Crear directorio temporal para MongoDB
    temp_dir = os.path.join(os.path.dirname(__file__), "temp_mongodb")
    os.makedirs(temp_dir, exist_ok=True)

    # Iniciar MongoDB con almacenamiento en memoria
    cmd = [
        "mongod",
        "--storageEngine",
        "inMemory",
        "--dbpath",
        temp_dir,
        "--port",
        str(MONGODB_PORT),
    ]

    try:
        proceso = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Dar tiempo para que MongoDB se inicie
        time.sleep(2)

        # Verificar que MongoDB se ha iniciado correctamente
        if proceso.poll() is not None:
            raise Exception("No se pudo iniciar MongoDB")

        return proceso
    except Exception as e:
        print(f"Error al iniciar MongoDB: {e}")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        return None


def crear_conexion() -> pymongo.database.Database:
    """
    Crea y devuelve una conexión a la base de datos MongoDB
    """
    try:
        # Crear conexión a MongoDB
        cliente = pymongo.MongoClient(f"mongodb://localhost:{MONGODB_PORT}/")

        # Obtener la base de datos específica
        db = cliente[DB_NAME]

        return db

    except Exception as e:
        print(f"Error al conectar con MongoDB: {e}")
        raise


def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.

    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # TODO: Implementar la creación de colecciones e índices
    pass


def insertar_autores(
    db: pymongo.database.Database, autores: List[Tuple[str]]
) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'

    Args:
        db: Objeto de conexión a la base de datos MongoDB
        autores: Lista de tuplas (nombre,)
    """
    # TODO: Implementar la inserción de autores
    try:
        # Preparar documentos para inserción
        documentos = []
        for nombre_tuple in autores:
            documento = {"nombre": nombre_tuple[0]}
            documentos.append(documento)

        # Insertar documentos y obtener IDs
        resultado = db.autores.insert_many(documentos)

        # Convertir ObjectId a string para compatibilidad
        ids_insertados = [str(doc_id) for doc_id in resultado.inserted_ids]

        print(f"Se insertaron {len(ids_insertados)} autores correctamente.")

        return ids_insertados

    except Exception as e:
        print(f"Error al insertar autores: {e}")
        return []


def insertar_libros(
    db: pymongo.database.Database, libros: List[Tuple[str, int, str]]
) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'

    Args:
        db: Objeto de conexión a la base de datos MongoDB
        libros: Lista de tuplas (titulo, anio, autor_id)
    """
    # TODO: Implementar la inserción de libros
    try:
        # Preparar documentos para inserción
        documentos = []
        for titulo, anio, autor_id_tuple in libros:
            documento = {
                "titulo": titulo,
                "anio": anio,
                "autor_id": str(
                    autor_id_tuple
                ),  # Convertir a string para compatibilidad
            }
            documentos.append(documento)

        # Insertar documentos y obtener IDs
        resultado = db.libros.insert_many(documentos)

        # Convertir ObjectId a string para compatibilidad
        ids_insertados = [str(doc_id) for doc_id in resultado.inserted_ids]

        print(f"Se insertaron {len(ids_insertados)} libros correctamente.")

        return ids_insertados

    except Exception as e:
        print(f"Error al insertar libros: {e}")
        return []


def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor

    Args:
        db: Objeto de conexión a la base de datos MongoDB
    """
    # TODO: Implementar la consulta de libros con sus autores
    try:
        # Agregación para obtener libros con información de autores
        pipeline = [
            {
                "$lookup": {
                    "from": "autores",
                    "localField": "autor_id",
                    "foreignField": "_id",
                    "as": "autor",
                }
            },
            {"$unwind": "$autor"},
            {"$project": {"titulo": 1, "anio": 1, "autor_nombre": "$autor.nombre"}},
            {"$sort": {"titulo": 1}},
        ]

        libros_con_autores = list(db.libros.aggregate(pipeline))

        print("\n=== LIBROS CON SUS AUTORES ===")
        for libro in libros_con_autores:
            print(f"- {libro['titulo']} ({libro['anio']}) - {libro['autor_nombre']}")

    except Exception as e:
        print(f"Error al consultar libros: {e}")


def buscar_libros_por_autor(
    db: pymongo.database.Database, nombre_autor: str
) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor

    Args:
        db: Objeto de conexión a la base de datos MongoDB
        nombre_autor: Nombre del autor a buscar

    Returns:
        Lista de tuplas (titulo, anio)
    """
    # TODO: Implementar la búsqueda de libros por autor
    try:
        # Buscar el autor por nombre
        autor = db.autores.find_one({"nombre": nombre_autor})

        if not autor:
            print(f"No se encontró el autor: {nombre_autor}")
            return []

        # Buscar libros del autor
        libros = list(
            db.libros.find(
                {"autor_id": str(autor["_id"])}, {"_id": 0, "titulo": 1, "anio": 1}
            )
        )

        # Convertir a lista de tuplas (titulo, anio)
        resultado = [(libro["titulo"], libro["anio"]) for libro in libros]

        return resultado

    except Exception as e:
        print(f"Error al buscar libros por autor: {e}")
        return []


def actualizar_libro(
    db: pymongo.database.Database,
    id_libro: str,
    nuevo_titulo: Optional[str] = None,
    nuevo_anio: Optional[int] = None,
) -> bool:
    """
    Actualiza la información de un libro existente

    Args:
        db: Objeto de conexión a la base de datos MongoDB
        id_libro: ID del libro a actualizar
        nuevo_titulo: Nuevo título del libro (opcional)
        nuevo_anio: Nuevo año de publicación (opcional)

    Returns:
        `True` si se actualizó correctamente, `False` si no se encontró el libro.

    """
    # TODO: Implementar la actualización de un libro
    try:
        # Construir el diccionario de actualización
        campos_actualizacion = {}

        if nuevo_titulo is not None:
            campos_actualizacion["titulo"] = nuevo_titulo

        if nuevo_anio is not None:
            campos_actualizacion["anio"] = nuevo_anio

        if not campos_actualizacion:
            print("No se proporcionaron campos para actualizar.")
            return False

        # Actualizar el documento
        resultado = db.libros.update_one(
            {"_id": pymongo.ObjectId(id_libro)}, {"$set": campos_actualizacion}
        )

        if resultado.modified_count > 0:
            print(f"Libro con ID {id_libro} actualizado correctamente.")
            return True
        else:
            print(f"No se encontró ningún libro con ID {id_libro}.")
            return False

    except Exception as e:
        print(f"Error al actualizar libro: {e}")
        return False


def eliminar_libro(db: pymongo.database.Database, id_libro: str) -> bool:
    """
    Elimina un libro por su ID

    Args:
        db: Objeto de conexión a la base de datos MongoDB
        id_libro: ID del libro a eliminar

    Returns:
        `True` si se eliminó correctamente, `False` si no se encontró el libro.
    """
    # TODO: Implementar la eliminación de un libro
    try:
        # Eliminar el documento
        resultado = db.libros.delete_one({"_id": pymongo.ObjectId(id_libro)})

        if resultado.deleted_count > 0:
            print(f"Libro con ID {id_libro} eliminado correctamente.")
            return True
        else:
            print(f"No se encontró ningún libro con ID {id_libro}.")
            return False

    except Exception as e:
        print(f"Error al eliminar libro: {e}")
        return False


def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de transacciones para operaciones agrupadas

    Args:
        db: Objeto de conexión a la base de datos MongoDB

    Returns:
        `True` si la transacción se completó correctamente, `False` en caso de error.
    """
    # TODO: Implementar un ejemplo de transacción
    try:
        # Iniciar sesión de transacción
        with db.client.start_session() as session:
            with session.start_transaction():
                # Agregar un nuevo autor
                nuevo_autor = {"nombre": "Octavio Paz"}
                resultado_autor = db.autores.insert_one(nuevo_autor, session=session)
                nuevo_autor_id = resultado_autor.inserted_id

                # Agregar un libro para ese autor
                nuevo_libro = {
                    "titulo": "El laberinto de la soledad",
                    "anio": 1950,
                    "autor_id": str(nuevo_autor_id),
                }
                resultado_libro = db.libros.insert_one(nuevo_libro, session=session)

                # Confirmar transacción (automático con 'with')
                print("Transacción completada: Se agregó 'Octavio Paz' y su libro.")
                return True

    except Exception as e:
        print(f"Error en la transacción: {e}")
        return False


if __name__ == "__main__":
    mongodb_proceso = None
    db = None

    try:
        # Verificar si MongoDB está instalado
        if not verificar_mongodb_instalado():
            print("Error: MongoDB no está instalado o no está disponible en el PATH.")
            print("Por favor, instala MongoDB y asegúrate de que esté en tu PATH.")
            sys.exit(1)

        # Iniciar MongoDB en memoria para el ejercicio
        print("Iniciando MongoDB en memoria...")
        mongodb_proceso = iniciar_mongodb_en_memoria()
        if not mongodb_proceso:
            print(
                "No se pudo iniciar MongoDB. Asegúrate de tener los permisos necesarios."
            )
            sys.exit(1)

        print("MongoDB iniciado correctamente.")

        # Crear una conexión
        print("Conectando a MongoDB...")
        db = crear_conexion()
        print("Conexión establecida correctamente.")

        # TODO: Implementar el código para probar las funciones

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            mongodb_proceso.terminate()
            mongodb_proceso.wait()

            # Eliminar directorio temporal
            temp_dir = os.path.join(os.path.dirname(__file__), "temp_mongodb")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            print("MongoDB detenido correctamente.")
