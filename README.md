# Data Pipelines with Airflow

## Project Overview
This project will create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

The project has four operators that implemented into functional pieces of a data pipeline. Also contains a set of tasks that linked to achieve a coherent and sensible data flow within the pipeline. And helpers class that contains all the SQL transformations to execute it with custom operators.

The source data resides in S3 and needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Datasets
Working with two datasets.the s3 links for each:

> Log data: s3://udacity-dend/log_data

> Song data: s3://udacity-dend/song_data

## Project files
The project contains three major components:
- The dag folder has all the imports and task templates in place, and task dependencies
- The operators folder with operator templates
- A helper class for the SQL transformations

### Project Operators:
1- Stage Operator
The stage operator to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
The parameters used to distinguish between JSON file. Also allows to load timestamped files from S3 based on the execution time and run backfills.

2- Fact Operator
Fact tables are usually so massive they only allow append type functionality. Most of the logic is within the SQL transformations and the operator is take as input a SQL statement and target database on which to run the query against.

3-Dimension Operators
Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. Also have a parameter that allows switching between insert modes when loading dimensions. Most of the logic is within the SQL transformations and the operator is take as input a SQL statement and target database on which to run the query against. 
 
4- Data Quality Operator
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator raise an exception and the task retry and fail eventually.
