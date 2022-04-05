class SqlQueries:
    covid_cases_create = '''
    CREATE TABLE IF NOT EXISTS covid19_daily_cases
        (
         date TIMESTAMP WITHOUT TIME ZONE,
         total INT,
         seoul INT,
         gyeonggi INT,
         incheon INT,
         CONSTRAINT PK_covid19_daily_cases PRIMARY KEY (date)
    )
    '''

    covid_cases_insert = '''
    INSERT INTO covid19_daily_cases
        VALUES ('%sT00:00:00' :: TIMESTAMP WITHOUT TIME ZONE, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT PK_covid19_daily_cases 
        DO UPDATE SET date = EXCLUDED.date,
            total = EXCLUDED.total,
            seoul = EXCLUDED.seoul,
            gyeonggi = EXCLUDED.gyeonggi,
            incheon = EXCLUDED.incheon;
    '''


    covid_vaccination_create = '''
    CREATE TABLE IF NOT EXISTS covid19_vaccination
        (
         date TIMESTAMP WITHOUT TIME ZONE,
         sido VARCHAR,
         totalFirstCnt INT,
         totalSecondCnt INT,
         totalThirdCnt INT,
         CONSTRAINT PK_covid19_vaccination PRIMARY KEY (date, sido)
    )
    '''

    covid_vaccination_insert = '''
    INSERT INTO covid19_vaccination
        VALUES ('%sT00:00:00' :: TIMESTAMP WITHOUT TIME ZONE, '%s', %s, %s, %s)
        ON CONFLICT ON CONSTRAINT PK_covid19_vaccination
        DO NOTHING;
    '''

    apple_index_create = '''
    CREATE TABLE IF NOT EXISTS apple_index
        (
         region VARCHAR,   
         method VARCHAR,    
         date TIMESTAMP WITHOUT TIME ZONE,
         index DOUBLE PRECISION,
         CONSTRAINT PK_apple_index PRIMARY KEY (date, region, method)
    )
    '''

    apple_index_insert = '''
    INSERT INTO apple_index
        VALUES ('%s', '%s','%sT00:00:00' :: TIMESTAMP WITHOUT TIME ZONE, '%s')
        ON CONFLICT ON CONSTRAINT PK_apple_index
        DO NOTHING;   
    '''

    seoul_living_migration_create = '''
    CREATE TABLE IF NOT EXISTS seoul_living_migration
        (base_month INT,
         dayofweek VARCHAR,
         desti_region_hour SMALLINT,
         origin_region_code INT,
         desti_region_code INT,
         sex VARCHAR(1),
         age_group INT,
         category VARCHAR(2),
         duration DOUBLE PRECISION,
         population DOUBLE PRECISION,
         CONSTRAINT PK_seoul_living_migration PRIMARY KEY (base_month, dayofweek, desti_region_hour, origin_region_code, desti_region_code, sex, age_group, category)
    )
    '''

    seoul_living_migration_insert = '''
     INSERT INTO seoul_living_migration
        VALUES (%s, '%s', %s, %s, %s,'%s', %s, '%s', %s, %s)
    '''

    drop_table = 'DROP TABLE IF EXISTS {}'