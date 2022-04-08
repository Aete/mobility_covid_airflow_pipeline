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
Please check ```requirement.txt```

## Data
![db diagram](./png.png)

### Seoul Living Migration
This table is the statistically deduced number of citizens' trips between neighborhoods by the hour, day of the week, sex, and trip category in each month.
- base_month: month and year ex) 202202
- dayofweek: day of the week ex) mon (=Monday)
- desti_region_hour: hour of arrival time ex) 7
- origin_region_code: code (id) of origin neighborhood (administrative 'dong')
- desti_region_code: code (id) of origin neighborhood (administrative 'dong')
- sex: M (Male) or F (Female)
- age_group: 5 years age group
- category: trip category consisted with two character (W: workplace, H: home, E: etc, ex) WW (workplace -> workplace), HW (home -> workplace)
- duration: duration of the trip
- population: deduced number of citizen trips between neighborhoods

### COVID 19 Daily new confirmed cases
This table is about the number of daily new confirmed cases in South Korea
- date: TIMESTAMP WITHOUT TIME ZONE
- total: the number of daily new confirmed cases in South Korea
- seoul: the number of daily new confirmed cases in Seoul
- gyeonggi: the number of daily new confirmed cases in Gyeonggi
- incheon: the number of the daily new confirmed cases in Incheon

### COVID 19 Daily Vaccination history
This table is about the number of cumulative vaccinated people in South Korea
- date: TIMESTAMP WITHOUT TIME ZONE
- sido: the name of the city (or province)
- totalFirstCnt : the number of the cumulative vaccinated people who were only vaccinated one time
- totalSecondCnt: the number of the cumulative vaccinated people who were only vaccinated twice
- totalThirdCnt: the number of the cumulative vaccinated people who were only vaccinated three times

### Apple mobility index
This table consists of the apple mobility index for Seoul
- region: Seoul,   
- method: walking or driving,    
- date: TIMESTAMP WITHOUT TIME ZONE,
- index: index score

### outlined data model (result)

In the project, there are four tables: 'seoul_living_migration', 'covid19_daily_cases', 'covid19_vaccination', and 'apple_index'. The Main issue is that temporal information of 'seoul_living_migration' data consists of the month and day of the week, but the other datasets were based on daily information. Therefore, there is a further process should be needed to unify the temporal scale of each table. For example, I merged all of the tables into monthly temporal scale like below:

|base_month|monthly_trips_count|avg_seoul_daily_cases|avg_seoul_vaccination|avg_apple_index|
|:--------:|:--------:|:-------------------:|:-------------------:|:-------------:|
|202202|800000000|17780.53|8186897.28|35.2957|

(you can check the result of the process in the log of the 'data_check_result' task after the all of tasks are done.  Currently, I handled a movement dataset for Feb. 2022 due to AWS cost. So, in the result of the last data check task, it looks like Feb. 2022 is the only month that can be analyzed. But, there are the dataset files between Jan. 2020 and Feb.2022 in the Seoul open data portal. Therefore I think that is not a problem if I can pay more only for the AWS S3 bucket.) 
