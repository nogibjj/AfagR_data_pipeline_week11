"""Query the database"""

import sqlite3


def select():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cfb_playersDB")
    print("We connected to cfb_playersDB table and fetched all the data:")
    print(cursor.fetchall())
    conn.close()
    return "Reading Success"


def update_query():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    # update execution
    cursor.execute(
        "UPDATE cfb_playersDB SET ranking = 0.9080 WHERE school = 'Paraclete' "
    )
    conn.commit()
    conn.close()
    return "Update Success"


def delete_query():
    conn = sqlite3.connect("cfb_playersDB.db")
    cursor = conn.cursor()
    # delete execution
    cursor.execute("DELETE FROM cfb_playersDB WHERE ranking = 0.9080 ")
    conn.commit()
    conn.close()
    return "Delete Success"


if __name__ == "__main__":
    select()
    update_query()
    delete_query()
