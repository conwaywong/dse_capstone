-- Phase 0
TRUNCATE Freeways CASCADE;
TRUNCATE County_City CASCADE;

-- Phase 1
TRUNCATE Station CASCADE;

-- Phase 2
TRUNCATE Observation CASCADE;

-- Phase 3
TRUNCATE Lane_Observation CASCADE;

-- Phase N
TRUNCATE CHP_INC CASCADE;
