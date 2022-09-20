from webbrowser import Opera
import psycopg2

from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None

    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Connection to Posgtres Successful")
    except OperationalError as e :
        print(f"The error '{e}' occurred")
    return connection


if __name__ == '__main__':
    connection = create_connection("acqdat_core_dev", "postgres", "postgres", "188.166.230.56", "5431")

    cursor = connection.cursor()
    query = "select * from acqdat_sensors_data limit 10"

    cursor.execute(query)
    records = cursor.fetchall()

    for row in records:
        print(row)
        print('\n')
