import mysql.connector
from mysql.connector import Error
from multiprocessing import Process, BoundedSemaphore, Value

# Función para conectar a la base de datos
def connection():
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="test"
        )
    except Error as ex:
        print(f"Error durante la conexión: {ex}")
        return None

# Función para obtener un rango de números desde la base de datos
def get_numbers(start, end):
    db = connection()
    if db is None:
        return []  # Si la conexión falla, devolvemos una lista vacía

    try:
        cursor = db.cursor()
        query = "SELECT numero FROM suma_paralela WHERE id >= %s AND id <= %s"
        cursor.execute(query, (start, end))
        numbers = cursor.fetchall()
        return [n[0] for n in numbers]
    except Error as ex:
        print(f"Error al obtener números: {ex}")
        return []
    finally:
        if cursor:
            cursor.close()
        if db.is_connected():
            db.close()

# Función para sumar un subconjunto de números
def sum_subset(start_end, shared_sum, semaphore):
    start, end = start_end
    numbers = get_numbers(start, end)
    local_sum = sum(numbers)

    # Imprimir la suma realizada por este proceso
    print(f'Proceso {start}-{end} suma local: {local_sum}')

    # Usar semáforo para evitar la concurrencia en la suma compartida
    with semaphore:
        shared_sum.value += local_sum

if __name__ == '__main__':
    # Ejemplo de asignación manual de rangos de trabajo
    # Puedes modificar esta lista con los rangos que desees asignar a cada proceso
    ranges = [
        (1, 100000),     # Proceso 1 suma los primeros 100000 números
        (100001, 300000), # Proceso 2 suma del 100001 al 300000
        (300001, 450000), # Proceso 3 suma del 300001 al 450000
        (450001, 600000), # Proceso 4 suma del 450001 al 600000
        (600001, 700000), # Proceso 5 suma del 600001 al 700000
        (700001, 750000), # Proceso 6 suma del 700001 al 750000
        (750001, 800000), # Proceso 7 suma del 750001 al 800000
        (800001, 850000), # Proceso 8 suma del 800001 al 850000
        (850001, 900000), # Proceso 9 suma del 850001 al 900000
        (900001, 1000000) # Proceso 10 suma del 900001 al 1000000
    ]

    # Crear la suma compartida y el semáforo
    shared_sum = Value('i', 0)  # Variable compartida entre procesos
    semaphore = BoundedSemaphore(1)  # Solo un proceso puede modificar la suma a la vez

    processes = []

    # Crear y comenzar los procesos
    for r in ranges:
        process = Process(target=sum_subset, args=(r, shared_sum, semaphore))
        processes.append(process)
        process.start()

    # Esperar a que todos los procesos terminen
    for process in processes:
        process.join()

    # Mostrar la suma total después de que todos los procesos hayan terminado
    print(f'La suma total de los números es: {shared_sum.value}')
