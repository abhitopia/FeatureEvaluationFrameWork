import datetime
from vertica_python import connect

VERTICA_CONNECTION = {'host': '10.1.24.75',
                      'port': 5433,
                      'user': 'a.aggarwal',
                      'password': 'admin01ABHI',
                      'database': 'dspr'}

query = """
DROP TABLE IF EXISTS valid_cookies;
CREATE LOCAL TEMPORARY TABLE valid_cookies
ON COMMIT PRESERVE ROWS
AS
SELECT
        DISTINCT cookie_id
FROM
        train.base_training_data
WHERE
        log_time_hour = '{LOG_TIME_HOUR}';


DROP TABLE IF EXISTS valid_clients;
CREATE LOCAL TEMPORARY TABLE valid_clients
ON COMMIT PRESERVE ROWS
AS
SELECT
        DISTINCT client_id
FROM
        train.base_training_data
WHERE
        log_time_hour = '{LOG_TIME_HOUR}';

DROP TABLE IF EXISTS cookie_categories;
CREATE LOCAL TEMPORARY TABLE cookie_categories
ON COMMIT PRESERVE ROWS
AS
SELECT
        cookie_id,
        public.concat_csv(category || ';' || (DATEDIFF('hour', log_time_hour, '{LOG_TIME_HOUR}') :: VARCHAR)) as categories
FROM
(
        SELECT
                c.cookie_id,
                c.domain,
                max(c.log_time_hour) as log_time_hour,
                c.category
        FROM
                experimental.categories AS c
        JOIN
                valid_cookies AS vc
        ON
                vc.cookie_id = c.cookie_id
        WHERE
                c.log_time_hour < ('{LOG_TIME_HOUR}' :: TIMESTAMP)
                AND c.log_time_hour >= ('{LOG_TIME_HOUR}' :: TIMESTAMP) - ( '30 days' :: INTERVAL)
        GROUP BY
                c.cookie_id,
                c.domain,
                c.category
) AS a
GROUP BY
        cookie_id;


DROP TABLE IF EXISTS client_categories;
CREATE LOCAL TEMPORARY TABLE client_categories
ON COMMIT PRESERVE ROWS
AS
SELECT 
        client_id,
        public.concat_csv(category || ';' || frequency :: VARCHAR) as categories
FROM 
(
        SELECT
                a.client_id,
                a.category,
                count(a.cookie_id) as frequency
        FROM
        (
                SELECT
                        c.client_id,
                        c.cookie_id,
                        max(c.log_time_hour) as log_time_hour,
                        c.category
                FROM
                        experimental.categories AS c
                JOIN
                        valid_clients AS vci
                ON
                        vci.client_id = c.client_id
                WHERE
                        c.log_time_hour < ('{LOG_TIME_HOUR}' :: TIMESTAMP)
                        AND c.log_time_hour >= ('{LOG_TIME_HOUR}' :: TIMESTAMP) - ( '30 days' :: INTERVAL)
                GROUP BY
                        c.client_id,
                        c.cookie_id,
                        c.category
        ) AS a
        GROUP BY
                a.client_id,
                a.category
) AS b
GROUP BY
        client_id;
        
       
--DROP TABLE IF EXISTS experimental.td_with_categories_2;
--CREATE TABLE experimental.td_with_categories_2
--AS
INSERT INTO experimental.td_with_categories_2
SELECT
        td.*,
        IFNULL(cc.categories, 'N/A') as cookie_categories,
        IFNULL(clc.categories, 'N/A') as client_categories
FROM
        train.base_training_data as td
LEFT JOIN
        cookie_categories as cc
ON
        cc.cookie_id = td.cookie_id
LEFT JOIN
        client_categories as clc
ON
        clc.client_id = td.client_id
WHERE
        td.log_time_hour = '{LOG_TIME_HOUR}'
        AND td.log_time < '{LOG_TIME_HOUR}' :: TIMESTAMP + '15 minutes' :: INTERVAL;

INSERT INTO experimental.td_with_categories_2
SELECT
        td.*,
        IFNULL(cc.categories, 'N/A') as cookie_categories,
        IFNULL(clc.categories, 'N/A') as client_categories
FROM
        train.base_training_data as td
LEFT JOIN
        cookie_categories as cc
ON
        cc.cookie_id = td.cookie_id
LEFT JOIN
        client_categories as clc
ON
        clc.client_id = td.client_id
WHERE
        td.log_time_hour = '{LOG_TIME_HOUR}'
        AND td.log_time >= '{LOG_TIME_HOUR}' :: TIMESTAMP + '15 minutes' :: INTERVAL
        AND td.log_time < '{LOG_TIME_HOUR}' :: TIMESTAMP + '30 minutes' :: INTERVAL;

INSERT INTO experimental.td_with_categories_2
SELECT
        td.*,
        IFNULL(cc.categories, 'N/A') as cookie_categories,
        IFNULL(clc.categories, 'N/A') as client_categories
FROM
        train.base_training_data as td
LEFT JOIN
        cookie_categories as cc
ON
        cc.cookie_id = td.cookie_id
LEFT JOIN
        client_categories as clc
ON
        clc.client_id = td.client_id
WHERE
        td.log_time_hour = '{LOG_TIME_HOUR}'
        AND td.log_time >= '{LOG_TIME_HOUR}' :: TIMESTAMP + '30 minutes' :: INTERVAL
        AND td.log_time < '{LOG_TIME_HOUR}' :: TIMESTAMP + '45 minutes' :: INTERVAL;

INSERT INTO experimental.td_with_categories_2
SELECT
        td.*,
        IFNULL(cc.categories, 'N/A') as cookie_categories,
        IFNULL(clc.categories, 'N/A') as client_categories
FROM
        train.base_training_data as td
LEFT JOIN
        cookie_categories as cc
ON
        cc.cookie_id = td.cookie_id
LEFT JOIN
        client_categories as clc
ON
        clc.client_id = td.client_id
WHERE
        td.log_time_hour = '{LOG_TIME_HOUR}'
        AND td.log_time >= '{LOG_TIME_HOUR}' :: TIMESTAMP + '45 minutes' :: INTERVAL
        AND td.log_time < '{LOG_TIME_HOUR}' :: TIMESTAMP + '60 minutes' :: INTERVAL;
"""

base_log_time_hour = datetime.datetime(2015, 06, 20, 0, 0, 0)

connection = connect(VERTICA_CONNECTION)
cursor = connection.cursor()
query_if_exists = """ SELECT count(*) FROM experimental.td_with_categories_2 WHERE log_time_hour = '{LOG_TIME_HOUR}'"""
for hour in range(8*24 + 1):
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