"""
Transforms and Loads data into the local SQLite3 database
Example:
,general name,count_products,ingred_FPro,avg_FPro_products,avg_distance_root,ingred_normalization_term,semantic_tree_name,semantic_tree_node
"""

import sqlite3
import csv
import os


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/usc_offers.csv"):
    """ "Transforms and Loads data into the local SQLite3 database"""

    # prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline=""), delimiter=",")
    next(payload)  # skip the first row of the csv (header)
    conn = sqlite3.connect("cfb_playersDB.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS cfb_playersDB")
    c.execute(
        "CREATE TABLE cfb_playersDB (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT, school TEXT,city TEXT,state TEXT, ranking INTEGER)"
    )
    # insert
    c.executemany(
        "INSERT INTO cfb_playersDB (name, school, city, state, ranking) VALUES (?, ?, ?, ?, ?)",
        payload,
    )
    conn.commit()
    conn.close()
    return "cfb_playersDB.db"
