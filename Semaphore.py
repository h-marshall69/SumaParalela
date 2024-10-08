import mysql.connector
from mysql.connector import Error
from multiprocessing import Process, BoundedSemaphore, Value, Manager

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
        # Asegúrate de que los números son enteros
        return [int(n[0]) for n in numbers]  # Convertir a int
    except Error as ex:
        print(f"Error al obtener números: {ex}")
        return []
    finally:
        if cursor:
            cursor.close()
        if db.is_connected():
            db.close()

# Función para sumar un subconjunto de números
def sum_subset(start_end, local_sums, semaphore):
    start, end = start_end
    numbers = get_numbers(start, end)
    local_sum = sum(numbers)

    # Imprimir la suma realizada por este proceso
    print(f'Proceso {start}-{end} suma local: {local_sum}\n')

    # Usar semáforo para evitar la concurrencia en la lista de sumas locales
    with semaphore:
        local_sums.append(local_sum)  # Agregar la suma local a la lista

if __name__ == '__main__':
    total_numbers = 1000000  # Solo usaremos 100000 números para la prueba
    num_processes = 10  # Número de procesos que se van a crear

    chunk_size = total_numbers // num_processes  # Calcular el tamaño de cada trozo

    # Crear los rangos para repartir los datos entre los procesos
    ranges = [(i * chunk_size + 1, (i + 1) * chunk_size) for i in range(num_processes)]

    # Asegurarse de que el último rango incluya todos los números restantes
    ranges[-1] = (ranges[-1][0], total_numbers)

    # Crear un gestor para manejar la lista de sumas locales y un semáforo
    manager = Manager()
    local_sums = manager.list()  # Lista compartida para almacenar sumas locales
    semaphore = BoundedSemaphore(1)  # Solo un proceso puede modificar la lista a la vez

    processes = []

    # Crear y comenzar los procesos
    for r in ranges:
        process = Process(target=sum_subset, args=(r, local_sums, semaphore))
        processes.append(process)
        process.start()

    # Esperar a que todos los procesos terminen
    for process in processes:
        process.join()

    # Sumar todas las sumas locales y mostrar la suma total
    total_sum = sum(local_sums)
    print(f'La suma total de los números es: {total_sum}')
