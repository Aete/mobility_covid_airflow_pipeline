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
         CONSTRAINT PK_apple PRIMARY KEY (date, method)
    )
    '''

    apple_index_insert = '''
    INSERT INTO apple_index
        VALUES (%s, %s,'%sT00:00:00' :: TIMESTAMP WITHOUT TIME ZONE, %s)
        ON CONFLICT ON CONSTRAINT PK_covid19_vaccination
        DO NOTHING;   
    '''

    drop_table = 'DROP TABLE IF EXISTS {}'