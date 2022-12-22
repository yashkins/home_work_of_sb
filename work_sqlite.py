import sqlite3
from sqlite3 import Error


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print('Соединение установленно')
    except Error as e:
        print(f'Error connection: {e}')
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print('Запрос выполнен')
    except Error as e:
        print(f'Error query: {e}')

def insert_employees(connection, query, values):
    cursor = connection.cursor()
    data = [(name,) for name in values]
    try:
        cursor.executemany(query, (data))
        connection.commit()
        print('Запрос выполнен')
    except Error as e:
        print(f'Error query: {e}')


def insert_Job_Days(connection, query, values):
    cursor = connection.cursor()
    data = []
    try:
        for name in values:
            column_names, result = select_empid(connection, query_select_empid, name)
            emp_id = result[0][0]
            for date in values[name]:
                data.append((emp_id, date))
        cursor.executemany(query, (data))
        connection.commit()
        print('Запрос выполнен')
    except Error as e:
        print(f'Error query: {e}')
    

def insert_losses(connection, query, values):
    cursor = connection.cursor()
    try:
        cursor.executemany(query, (values))
        connection.commit()
        print('Запрос выполнен')
    except Error as e:
        print(f'Error query: {e}')


def select_empid(connection, query, name):
    cursor = connection.cursor()
    result = None
    descriptions = None
    try:
        cursor.execute(query, (name,))
        descriptions = [descript[0] for descript in cursor.description]
        result = cursor.fetchall()
    except Error as e:
        print(f'Error query: {e}')
    return descriptions, result


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    descriptions = None
    try:
        cursor.execute(query)
        descriptions = [descript[0] for descript in cursor.description]
        result = cursor.fetchall()
    except Error as e:
        print(f'Error query: {e}')
    return descriptions, result


query_create_emp = """CREATE TABLE IF NOT EXISTS Employees(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
        );"""

query_create_jd = """CREATE TABLE IF NOT EXISTS Job_Deys(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        emp_id INTEGER NOT NULL,
        date timestamp NOT NULL,
        FOREIGN KEY(emp_id) REFERENCES Employees(id)
        );"""

query_create_loss =  """CREATE TABLE IF NOT EXISTS Losses(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date timestamp NOT NULL,
        emount REAL NOT NULL
        );"""

query_insert_emp = """INSERT INTO Employees(name) VALUES (?)"""

query_insert_jd = """INSERT INTO Job_Deys(emp_id, date) VALUES (?,?)"""

query_insert_loss = """INSERT INTO Losses(date, emount) VALUES (?,?)"""

query_select_empid = """SELECT id FROM Employees WHERE name = ?"""

query_select_result = """SELECT name as ФИО, COUNT(jd.date) as Колличество 
        FROM Employees as emp INNER JOIN Job_Deys as jd ON emp.id = jd.emp_id
        INNER JOIN Losses as loss ON jd.date = loss.date
        GROUP BY name"""

query_select_result_distinct = """SELECT name as ФИО, COUNT(DISTINCT jd.date) as Колличество 
        FROM Employees as emp INNER JOIN Job_Deys as jd ON emp.id = jd.emp_id
        INNER JOIN Losses as loss ON jd.date = loss.date
        GROUP BY name"""





