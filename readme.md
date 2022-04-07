# Mobility & COVID19 airflow pipeline

This project has been done as a capstone for Udacity Data Engineering Nanodegree.

## Introduction

This project aims to build a PostgreSQL database of COVID-19 and mobility in Seoul City, South Korea. <br/>

For this purpose, I used Apache Airflow to collect tables from various sources like Public API and Amazon S3. Some tables will be transformed with Pandas while it is being loaded in Airflow tasks.

After building the ETL (I mean if I can pass this capstone), my plan is to write a report about the relationship between daily movements, confirmed cases, and the number of vaccinated people.

### Why airflow?
I am thinking about an automated ETL pipeline based on its interval time. At this time, I used the monthly dataset because the city government of Seoul uploaded monthly movements data only on their open data portal. So, I was thinking about the ETL pipeline that updates a Postgres database monthly.   

### Addressing Other Scenarios
1) The data was increased by 100x. : If I have to handle 100 times bigger datasets for the project, I will use Apache Spark or Dask to do the distributed computing. Also, I will consider using AWS computing power and storage because my laptop will not be able to handle the dataset. 
2) The pipelines would be run on a daily basis by 7 am every day. : I can set the specific time and interval of updating for the data pipeline in Airflow
3) The database needed to be accessed by 100+ people. : I will use the Amazon Redshift or S3 (serve as CSV formatted file) instead of the local Postgres server.

## Install / Clone
To clone this project, do this:
```git clone https://github.com/Aete/mobility_covid_airflow_pipeline.git ```

### Dependancy

## Data

### Seoul Living Migration

### COVID 19 Daily new confirmed cases

### COVID 19 Daily Vaccination history

### Apple mobility index
