import psycopg2
from faker import Faker

# Подключение к базе данных PostgreSQL (обязательно только через psycopg2).
conn = psycopg2.connect(
    dbname="database",
    user="username",
    password="secret",
    host="db",
    port="5432"
)


def create_employers_table(conn):
    """
    Создание таблицы 'employers' в PostgreSQL базе данных если она не существует.

    Args:
        conn (psycopg2 connection): Соединение с PostgreSQL базой.

    Returns:
        None
    """
    # Создание таблицы.
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employers (
            ID SERIAL PRIMARY KEY,
            Name VARCHAR(255),
            Age INTEGER,
            Department VARCHAR(255)
        );
    """)
    conn.commit()


def inserts_employers_table(conn, num_rows):
    """
    Наполнение таблицы данными.

    Args:
        conn (psycopg2 connection): Соединение с PostgreSQL базой.
        num_rows (int): Количество строк добавляемых в таблицу.

    Returns:
        None
    """
    fake = Faker("ru_RU")
    cur = conn.cursor()
    for _ in range(num_rows):
        cur.execute("""
            INSERT INTO employers (Name, Age, Department)
            VALUES (%s, %s, %s);
        """, (fake.name(), fake.random_int(min=20, max=69), fake.random_element(elements=("Бухгалтерия", "Отдел кадров", "Административный", "Деканат Технологический", "Деканат Экологический"))))
    conn.commit()


def print_employers_table(conn, num_rows):
    """
    Вывод данных из 'employers' таблицы PostgreSQL базы.

    Args:
        conn (psycopg2 connection): Соединение с PostgreSQL базой.
        num_rows (int): Количество выводимых строк

    Returns:
        None
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM employers LIMIT %s;
    """, (num_rows,))
    rows = cur.fetchall()
    for row in rows:
        print("ID:%d, Name:%s , Age:%d , Department:%s " % (row[0], row[1], row[2], row[3]))

if __name__ == '__main__':
    create_employers_table(conn)
    inserts_employers_table(conn,30)
    print_employers_table(conn, 25)
