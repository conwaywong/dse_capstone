-- PEMS Schema
SELECT COUNT(*) AS District FROM District;
SELECT COUNT(*) AS ST_Type FROM ST_Type;
SELECT COUNT(*) AS Freeways FROM Freeways;
SELECT COUNT(*) AS County_City FROM County_City;
SELECT COUNT(*) AS Traffic_Station FROM Traffic_Station;
SELECT COUNT(*) AS Observation FROM Observation;
SELECT COUNT(*) AS Lane_Observation FROM Lane_Observation;
SELECT COUNT(*) AS CHP_Desc FROM CHP_Desc;
SELECT COUNT(*) AS CHP_INC FROM CHP_INC;
SELECT COUNT(*) AS Weather_Station FROM Weather_Station;
SELECT COUNT(*) AS Precipitation_Hourly_Observation FROM Precipitation_Hourly_Observation;
SELECT COUNT(*) AS Precipitation_Daily_Total FROM Precipitation_Daily_Total;

SELECT * FROM District LIMIT 1;
SELECT * FROM ST_Type LIMIT 1;
SELECT * FROM Freeways LIMIT 1;
SELECT * FROM County_City LIMIT 1;
SELECT * FROM Traffic_Station LIMIT 1;
SELECT * FROM Observation LIMIT 1;
SELECT * FROM Lane_Observation LIMIT 1;
SELECT * FROM CHP_Desc LIMIT 1;
SELECT * FROM CHP_INC LIMIT 1;
SELECT * FROM Weather_Station LIMIT 1;
SELECT * FROM Precipitation_Hourly_Observation LIMIT 1;
SELECT * FROM Precipitation_Daily_Total LIMIT 1;
