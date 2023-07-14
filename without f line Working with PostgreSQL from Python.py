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


def add_client(conn, name, surname, e_mail):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO Client(name, surname, e_mail)
            VALUES(%s, %s, %s)
            RETURNING client_id, name, surname, e_mail;
            """, (name, surname, e_mail))
        return cur.fetchall()


def add_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO Phone (client_id, number)
            VALUES (%s,%s)
            RETURNING client_id, number;
            """, (client_id, number))
        return cur.fetchall()


def change_client(conn, client_id, name=None, surname=None, e_mail=None):
    with conn.cursor() as cur:
        update_fields = []
        query_params = []

        if name is not None:
            update_fields.append("name = %s")
            query_params.append(name)
        if surname is not None:
            update_fields.append("surname = %s")
            query_params.append(surname)
        if e_mail is not None:
            update_fields.append("e_mail = %s")
            query_params.append(e_mail)

        if len(update_fields) > 0:
            set_clause = ", ".join(update_fields)
            update_query = "UPDATE Client SET {} WHERE client_id = %s RETURNING client_id, name, surname, e_mail;".format(set_clause)
            query_params.append(client_id)

            cur.execute(update_query, tuple(query_params))
            return cur.fetchall()
        else:
            return None



def change_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE Phone
            SET number = %s
            WHERE client_id = %s
            RETURNING client_id, number;
            """, (number, client_id))
        return cur.fetchall()


def delete_phone(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM Phone
            WHERE client_id = %s
            RETURNING client_id, number;
            """, (client_id,))
        return cur.fetchall()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM Client
            WHERE client_id = %s
            RETURNING client_id, name, surname, e_mail;
            """, (client_id,))
        return cur.fetchall()


def find_client(conn, name=None, surname=None, e_mail=None, number=None):
    with conn.cursor() as cur:
        query = """
            SELECT c.name, c.surname, c.e_mail, p.number 
            FROM Client AS c
            LEFT JOIN Phone AS p ON c.client_id = p.client_id
            WHERE 1 = 1
            """
        query_params = []

        if name is not None:
            query += "AND c.name = %s "
            query_params.append(name)
        if surname is not None:
            query += "AND c.surname = %s "
            query_params.append(surname)
        if e_mail is not None:
            query += "AND c.e_mail = %s "
            query_params.append(e_mail)
        if number is not None:
            query += "AND p.number = %s "
            query_params.append(number)

        cur.execute(query, query_params)
        return cur.fetchall()


if __name__ == "__main__":
    conn = psycopg2.connect(database="clients", user="postgres", password="041953")
    create_db(conn)

    print(add_client(conn, 'Peter', 'Peterson', 'pet@mail.ru'))
    print(add_client(conn, 'Nika', 'Ivanova', 'ivan@gmail.com'))
    print(add_client(conn, 'Alex', 'Groten', 'joni@gmail.com'))
    print(add_phone(conn, 1, '89508721563'))
    print(add_phone(conn, 2, '89203215477'))
    print(add_phone(conn, 3, '89103628791'))
    print(change_client(conn, 1, surname='Peterson', name='Peter'))
    print(change_phone(conn, 1, '89622567483'))
    print(delete_phone(conn, 3))
    print(delete_client(conn, 3))
    print(find_client(conn, name='Nika'))

    conn.commit()
    conn.close()
