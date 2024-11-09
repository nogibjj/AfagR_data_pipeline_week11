[![CI](https://github.com/nogibjj/AfagR_PySpark_week10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/AfagR_PySpark_week10/actions/workflows/cicd.yml)

## Mini Project 10 

The repository involves utilizing PySpark for data processing on a dataset. The main objectives are to incorporate a Spark SQL query and execute a data transformation.



## Requirements
* Use PySpark to perform data processing on a large dataset.</br>
* Include at least one Spark SQL query and one data transformation.</br>

#### If you don't have pyspark installed you can follow below steps to use the repo succesfully and work with pyspark

- open codespaces
- wait for environment to be installed
- run: python main.py

## Data
- I have used football players and their ranking data you can find the dataset here : 
- [cfb_players.csv](https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv)

## Process 

- Extract the dataset via extract
- Start a spark session via start_spark
- Load the dataset via load_data
- Find some descriptive statistics via descibe
- Query the dataset via query
- Transformation on the sample dataset via example_transform
- End spark session via end_spark

#### You can find results of the process in *pyspark_output.md* file

