WITH RankedRows AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY <ordering_column>) AS rn
    FROM VYTALIZE_0171.PUBLIC.TRAININGFEATURESPSI
)
SELECT 
    ANY_VALUE(CASE WHEN rn = 1 THEN 'Yes' ELSE 'No' END) AS first_row_exceeds
FROM RankedRows
WHERE rn = 1 
  AND (column1 > 100 OR column2 > 100 OR column3 > 100);

CREATE OR REPLACE ALERT psi_threshold_alert
  WAREHOUSE = my_warehouse
  SCHEDULE = 'USING CRON */1000 * * * * UTC' -- Checks every 10 minutes
  IF (EXISTS (
      SELECT 1 
      FROM VYTALIZE_0171.PUBLIC.TRAININGFEATURESPSI 
      WHERE psi_value > 0.5 
        AND INSERT_TIMESTAMP >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'my_email_int',
    'your_email@example.com',
    'Snowflake Alert: PSI Threshold Exceeded',
    'One or more PSI values have exceeded the 0.5 threshold in the last 10 minutes.'
  );

