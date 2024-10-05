## Python Script interacting with SQL Database

[![CI](https://github.com/nogibjj/AfagR_DE_assignment5/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/AfagR_DE_assignment5/actions/workflows/cicd.yml)
![4 17-etl-sqlite-RAW](https://github.com/nogibjj/sqlite-lab/assets/58792/b39b21b4-ccb4-4cc4-b262-7db34492c16d)


### Project Data Source:
Repository forked from the https://github.com/nogibjj/sqlite-lab 
https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv


### Project components

* ETL-Query:  [E] Extract a dataset from URL, [T] Transform, [L] Load into SQLite Database.
* [E] Extract a dataset from a URL like Kaggle or data.gov. JSON or CSV formats tend to work well.
* [T] Transform the data by cleaning, filtering, enriching, etc to get it ready for analysis.
* [L] Load the transformed data into a SQLite database table using Python's sqlite3 module.
* CRUD operations
* main.py -  for executing the functions that we created
* test_main.py - for testing the functions
* requirements file 


### Data Extraction

![4 17-etl-sqlite-R](img.png)
Above is the table format and data in it. 
•	Load CSV data into SQLite database.
•	Create and manage a SQLite database.


### CRUD Operations
read , update, delete from the created table

![4 17-etl-sqlite-R](image2.png)


### Test all the steps
![4 17-etl-sqlite-RAW](image.png)