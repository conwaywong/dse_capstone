-- Phase 0
TRUNCATE Freeways CASCADE;
TRUNCATE County_City CASCADE;
TRUNCATE Weather_Station CASCADE;
TRUNCATE CHP_Desc CASCADE;

-- Phase 1
TRUNCATE Traffic_Station CASCADE;
TRUNCATE CHP_INC CASCADE;
TRUNCATE Precipitation_Hourly_Observation CASCADE;
TRUNCATE Precipitation_Daily_Total CASCADE;

-- Phase 2
TRUNCATE Observation CASCADE;

-- Phase 3
TRUNCATE Lane_Observation CASCADE;
