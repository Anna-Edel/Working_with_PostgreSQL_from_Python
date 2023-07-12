import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS Phone;
        DROP TABLE IF EXISTS Client;
        """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS Client(
                client_id SERIAL PRIMARY KEY,
                name VARCHAR(60) NOT NULL,
                surname VARCHAR(60) NOT NULL,
                e_mail VARCHAR(60) NOT NULL UNIQUE);
                """)

        cur.execute("""
                CREATE TABLE IF NOT EXISTS Phone(
                number DECIMAL UNIQUE CHECK(number <= 99999999999),
                client_id INTEGER REFERENCES Client(client_id));
                """)
        conn.commit()


def add_client(conn, name, surname, e_mail, phones=None):
    conn.execute("""
            INSERT INTO Client(name, surname, e_mail)
            VALUES(%s, %s, %s)
            RETURNING client_id, name, surname, e_mail;
            """, (name, surname, e_mail))
    return cur.fetchall()


def add_phone(conn, client_id, number):
    conn.execute("""
    INSERT INTO Phone (client_id, number)
    VALUES (%s,%s)
    RETURNING client_id, number;
    """, (client_id, number))
    return cur.fetchall()


def change_client(conn, client_id, name=None, surname=None, e_mail=None, number=None):
    conn.execute("""
            UPDATE Client
            SET name=%s, surname=%s, e_mail=%s
            WHERE client_id=%s
            RETURNING client_id, name, surname, e_mail;
            """, (name, surname, e_mail, client_id))
    return cur.fetchall()


def change_phone(conn, client_id, number):
    conn.execute("""
            UPDATE Phone
            SET number=%s
            WHERE client_id=%s
            RETURNING client_id, number;
            """, (number, client_id))
    return cur.fetchall()


def delete_phone(conn, client_id, number=None):
    conn.execute("""
            DELETE FROM Phone
            WHERE client_id=%s
            """, (client_id,))
    conn.execute("""
            SELECT * FROM Phone
            """, (number, client_id))
    return cur.fetchall()


def delete_client(conn, client_id, name=None, surname=None, e_mail=None, number=None):
    conn.execute("""
            DELETE FROM Client
            WHERE client_id=%s
            """, (client_id,))
    conn.execute("""
            SELECT * FROM Client
            """, (name, surname, e_mail, client_id))
    return cur.fetchall()


def find_client(conn, name=None, surname=None, e_mail=None, number=None):
    conn.execute("""
            SELECT c.name, c.surname, c.e_mail, p.number FROM Client AS c
            LEFT JOIN Phone AS p ON c.client_id = p.client_id
            WHERE c.name=%s OR c.surname=%s OR c.e_mail=%s OR p.number=%s;
            """, (name, surname, e_mail, number))
    return cur.fetchall()


conn = psycopg2.connect(database="clients", user="postgres", password="041953")

create_db(conn)
with conn.cursor() as cur:
    print(add_client(cur, 'Peter', 'Peterson', 'pet@mail.ru'))
    print(add_client(cur, 'Nika', 'Ivanova', 'ivan@gmail.com'))
    print(add_client(cur, 'Alex', 'Groten', 'joni@gmail.com'))
    print(add_phone(cur, '1', '89508721563'))
    print(add_phone(cur, '2', '89203215477'))
    print(add_phone(cur, '3', '89103628791'))
    print(change_client(cur, '1', 'Peterson', 'Peter', 'pet@mail.ru'))
    print(change_phone(cur, '1', '89622567483'))
    print(delete_phone(cur, '3'))
    print(delete_client(cur, '3'))
    print(find_client(cur, 'Nika'))
    conn.commit()
