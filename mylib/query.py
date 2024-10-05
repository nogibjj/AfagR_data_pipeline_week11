"""Query the database"""

import sqlite3


def select():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cfb_playersDB limit 10")
    print("We connected to cfb_playersDB table and fetched 10 rows:")
    print(cursor.fetchall())
    print("Done Select")
    conn.close()
    return "Reading Success"


def update_query():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    # update execution
    cursor.execute("UPDATE cfb_playersDB SET ranking = 0.9080 WHERE id = 1 ")
    conn.commit()
    conn.close()
    return "Update Success"


def delete_query():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    # delete execution
    cursor.execute("DELETE FROM cfb_playersDB WHERE id = 10 ")
    conn.commit()
    conn.close()
    return "Delete Success"


if __name__ == "__main__":
    select()
    update_query()
    delete_query()
