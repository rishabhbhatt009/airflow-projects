import os 
import json
import mysql.connector
from mysql.connector import Error

def get_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
            db_config = config.get('database', {})
            host = db_config.get('hostname')
            database = db_config.get('database')
            username = db_config.get('username')
            password = db_config.get('password')

            return {
                "hostname": host,
                "database": database,
                "username": username,
                "password": password
            }
    except FileNotFoundError:
        print("File not found.")
        return None
    except json.JSONDecodeError:
        print("Error decoding JSON.")
        return None


def disconnect(connection):
    # Close the connection
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")


def test_aurora_connection(host, username, password): 
    try:
        print('Connecting to MySQL Server')
        connection = mysql.connector.connect(host=host, user=username, password=password)
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Successfully connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute('SHOW DATABASES;')
            result = cursor.fetchall()
            print(f'{len(result)} Database available : {" | ".join([i[0] for i in result])}')
    
    except Error as e:
        print("Error while connecting to MySQL")
        print(e)

    finally:
        # Close the connection
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")


def connect_db(host, username, password): 
    try:
        print('Connecting to MySQL Server')
        connection = mysql.connector.connect(host=host, user=username, password=password)
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Successfully connected to MySQL Server version {db_info}")
            return connection
    except Error as e:
        print("Error while connecting to MySQL")
        print(e)


def generate_server_schema(connection, path):
    cursor = connection.cursor()
    with open(os.path.join(path,'server_schema.txt'),'w') as file : 
        file.write('|'.join(['database', 'table', 'col_name','col_type']))
        cursor.execute('SHOW DATABASES;')
        databases = cursor.fetchall()
        for (database,) in databases :
            cursor.execute(f'USE {database}')
            cursor.execute('SHOW TABLES')
            for (table,) in cursor.fetchall():
                cursor.execute(f'DESCRIBE {table}')
                for (col_name,col_type,*args) in cursor.fetchall():
                    file.write('|'.join([database, table, col_name,col_type]))
                    file.write('\n')


def run_sql_script(connection, script):
    cursor = connection.cursor()
    with open(script, 'r') as file:
        sql_script = file.read()
    try:
        for result in cursor.execute(sql_script, multi=True):
            if result.with_rows:
                print("Query result:")
                for row in result:
                    print(row)
            else:
                print(f"Rows affected by '{result.statement}': {result.rowcount}")
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()


if '__name__' == '__main__':
    test_aurora_connection()