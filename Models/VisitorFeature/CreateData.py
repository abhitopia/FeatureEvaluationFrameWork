import datetime
from vertica_python import connect

VERTICA_CONNECTION = {'host': '10.1.24.75',
                      'port': 5433,
                      'user': 'dbadmin',
                      'password': 'MUEKGZ4edpSw2zGFXt2eht',
                      'database': 'dspr'}

query = """
DROP TABLE IF EXISTS vc;
CREATE LOCAL TEMPORARY TABLE vc
ON COMMIT PRESERVE ROWS
AS
SELECT
        distinct cookie_id
FROM
        train.base_training_data
WHERE
        log_time_hour = '{LOG_TIME_HOUR}';

DROP TABLE IF EXISTS mo;
CREATE LOCAL TEMPORARY TABLE mo
ON COMMIT PRESERVE ROWS
AS
SELECT
        feature_value,
        schedule_time,
        coeff
FROM (
        SELECT
                feature_value,
                schedule_time,
                coeff,
                ROW_NUMBER() OVER (PARTITION BY feature_value ORDER BY schedule_time DESC) AS row
        FROM
                train.model_output
        WHERE
                feature_name = 'visited_domains' AND
                schedule_time < '{LOG_TIME_HOUR}'
      ) AS a
WHERE
        row = 1;

SELECT analyze_statistics('mo');

DROP TABLE IF EXISTS cfo;
CREATE LOCAL TEMPORARY TABLE cfo
ON COMMIT PRESERVE ROWS
AS
SELECT
        cfo.*,
        mo.*
FROM
        fact.cookie_feature_observations AS cfo
JOIN
        mo
ON
        mo.feature_value = cfo.cookie_feature_id
JOIN
        vc
ON
        vc.cookie_id = cfo.cookie_id
WHERE
        cfo.cookie_feature_type_name_id = 1 AND
        cfo.cookie_feature_observed_time < '{LOG_TIME_HOUR}';


SELECT analyze_statistics('cfo');

DROP TABLE IF EXISTS vd_features;
CREATE LOCAL TEMPORARY TABLE vd_features
ON COMMIT PRESERVE ROWS
AS
SELECT
         cookie_id,
         concat_csv( (DATEDIFF('hour', cookie_feature_observed_time, '{LOG_TIME_HOUR}') :: VARCHAR) || '=' || (coeff :: VARCHAR) ) AS vd_hour_coeffs
FROM
        cfo
GROUP BY
        cookie_id;

--DROP TABLE IF EXISTS temp.td_abhi;
--CREATE TABLE temp.td_abhi
--AS
INSERT INTO temp.td_abhi
SELECT
        td.*,
        vdf.vd_hour_coeffs
FROM
        train.base_training_data AS td
LEFT JOIN
        vd_features vdf
ON
        vdf.cookie_id = td.cookie_id
WHERE
        td.log_time_hour = '{LOG_TIME_HOUR}';"""

base_log_time_hour = datetime.datetime(2015, 05, 8, 7, 0, 0)

connection = connect(VERTICA_CONNECTION)
cursor = connection.cursor()
query_if_exists = """ SELECT count(*) FROM temp.td_abhi WHERE log_time_hour = '{LOG_TIME_HOUR}'"""
for hour in range(24):
    log_time_hour = str(base_log_time_hour + datetime.timedelta(hours=hour))
    query_to_execute = query_if_exists.format(formatTYPE=type, LOG_TIME_HOUR=log_time_hour)
    print 'Checking if data exists for ', log_time_hour
    print query_to_execute
    cursor.execute(query_to_execute)
    val = cursor.fetchone()
    if val[0] < 10:
        query_to_execute = query.format(formatTYPE=type, LOG_TIME_HOUR=log_time_hour)
        print query_to_execute
        cursor.execute(query_to_execute)