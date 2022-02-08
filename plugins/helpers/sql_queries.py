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

    drop_table = 'DROP TABLE IF EXISTS {}'