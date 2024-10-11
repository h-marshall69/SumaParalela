from mysql.connector import Error
import mysql.connector

try:
    # Conectar a la base de datos
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="test"
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Eliminar la tabla si ya existe
        cursor.execute("DROP TABLE IF EXISTS suma_paralela")

        # Crear la tabla nuevamente
        cursor.execute("""
            CREATE TABLE suma_paralela (
                id INT AUTO_INCREMENT PRIMARY KEY,
                numero INT NOT NULL
            )
        """)

        # Inserción de números en partes
        total_numeros = 1000000
        batch_size = 10000
        for i in range(1, total_numeros + 1, batch_size):
            # Crear una lista de tuplas para el lote actual
            numeros = [(j,) for j in range(i, min(i + batch_size, total_numeros + 1))]
            cursor.executemany("INSERT INTO suma_paralela (numero) VALUES (%s)", numeros)
            connection.commit() 
            print(f"Registros insertados en lote desde {i} hasta {min(i + batch_size - 1, total_numeros)}: {cursor.rowcount}")

except Error as ex:
    print("Error durante la conexión: {}".format(ex))
finally:
    if connection.is_connected():
        cursor.close()  
        connection.close() 
        print("Conexión cerrada.")
