-- PEMS Schema
-- Random Sampling:
--		http://stackoverflow.com/questions/8674718/best-way-to-select-random-rows-postgresql/8675160#8675160
--		http://dba.stackexchange.com/questions/96610/sampling-in-postgresql

-- Sunrise/Sunset:
--		https://en.wikipedia.org/wiki/Sunrise_equation
--		https://pypi.python.org/pypi/astral
--		http://michelanders.blogspot.ru/2010/12/calulating-sunrise-and-sunset-in-python.html
--		http://rhodesmill.org/pyephem/
--		https://github.com/mikereedell/sunrisesunsetlib-java

-- Phase 0
DROP TABLE IF EXISTS District CASCADE;
CREATE TABLE District (
	ID SERIAL PRIMARY KEY,
	NAME TEXT NOT NULL
);

INSERT INTO District VALUES (1 , 'Northwest');
INSERT INTO District VALUES (2 , 'Northeast');
INSERT INTO District VALUES (3 , 'North Central');
INSERT INTO District VALUES (4 , 'Bay Area');
INSERT INTO District VALUES (5 , 'Central Coast');
INSERT INTO District VALUES (6 , 'South Central');
INSERT INTO District VALUES (7 , 'LA/Ventura');
INSERT INTO District VALUES (8 , 'San Bernardino/Riverside');
INSERT INTO District VALUES (9 , 'Eastern Sierra');
INSERT INTO District VALUES (10, 'Central');
INSERT INTO District VALUES (11, 'San Diego/Imperial');
INSERT INTO District VALUES (12, 'Orange County');

DROP TABLE IF EXISTS ST_Type CASCADE;
CREATE TABLE ST_Type (
	ID SERIAL PRIMARY KEY,
	Type TEXT NOT NULL,
	Description TEXT NOT NULL
);

INSERT INTO ST_Type VALUES (1 ,'CD', 'Coll/Dist');
INSERT INTO ST_Type VALUES (2 ,'CH', 'Conventional Highway');
INSERT INTO ST_Type VALUES (3 ,'FF', 'Fwy-Fwy connector');
INSERT INTO ST_Type VALUES (4 ,'FR', 'Off Ramp');
INSERT INTO ST_Type VALUES (5 ,'HV', 'HOV');
INSERT INTO ST_Type VALUES (6 ,'ML', 'Mainline');
INSERT INTO ST_Type VALUES (7 ,'OR', 'On Ramp');

DROP TABLE IF EXISTS Freeways CASCADE;
CREATE TABLE Freeways (
	ID SERIAL PRIMARY KEY,
	Num INTEGER NOT NULL,
	Direction TEXT NOT NULL
);

CREATE INDEX Fwy_Num_Dir ON Freeways(Num, Direction);

DROP TABLE IF EXISTS County_City CASCADE;
CREATE TABLE County_City (
	ID SERIAL PRIMARY KEY,
	County_FIPS_ID INTEGER NOT NULL,
	City_FIPS_ID INTEGER
);

-- Phase 1
DROP TABLE IF EXISTS Traffic_Station CASCADE;
CREATE TABLE Traffic_Station (
	ID SERIAL PRIMARY KEY,
	PEMS_ID BIGINT,
	Effective_Start Date NOT NULL,
	Effective_End Date,
	Name TEXT,
	Fwy_ID INTEGER NOT NULL REFERENCES Freeways(ID),
	CCID_ID INTEGER NOT NULL REFERENCES County_City(ID),
	District_ID INTEGER NOT NULL REFERENCES District(ID),
	State_PM FLOAT NOT NULL,
	ABS_PM FLOAT NOT NULL,
	Latitude FLOAT NOT NULL,
	Longitude FLOAT NOT NULL,
	Location geography(POINT,4326) NOT NULL,
	Length FLOAT,
	Type_ID INTEGER NOT NULL REFERENCES ST_Type(ID),
	Num_Lanes INTEGER NOT NULL
	-- USER_ID DELETE
);

-- Phase 2
DROP TABLE IF EXISTS Observations CASCADE;
CREATE TABLE Observations (
	Station_ID INTEGER NOT NULL REFERENCES Traffic_Station(ID),
	District_ID INTEGER NOT NULL REFERENCES District(ID),
	Year SMALLINT,
	DOY SMALLINT,
	Flow_Coef FLOAT[10],
	Occupancy_Coef FLOAT[10],
	Speed_Coef FLOAT[10]
);

CREATE UNIQUE INDEX Observations_Idx ON Observations(Station_ID, Year, DOY);

ALTER TABLE Observations
	DROP COLUMN Occupancy_Coef,
	DROP COLUMN Speed_Coef;

ALTER TABLE Observations
	ADD COLUMN Weekend_Coef FLOAT[5],
	ADD COLUMN Weekday_Coef FLOAT[5];

DROP TABLE IF EXISTS Observations_Unknown CASCADE;
CREATE TABLE Observations_Unknown (
	Station_ID INTEGER NOT NULL REFERENCES Traffic_Station(ID),
	District_ID INTEGER NOT NULL REFERENCES District(ID),
	Year SMALLINT,
	DOY SMALLINT,
	Flow_Coef FLOAT[10],
	Occupancy_Coef FLOAT[10],
	Speed_Coef FLOAT[10]
);

ALTER TABLE Observations_Unknown
	DROP COLUMN Occupancy_Coef,
	DROP COLUMN Speed_Coef;

-- Phase 3
DROP TABLE IF EXISTS CHP_Desc CASCADE;
CREATE TABLE CHP_Desc (
	ID TEXT PRIMARY KEY,
	Description TEXT
);

-- Phase 3
DROP TABLE IF EXISTS CHP_Desc CASCADE;
CREATE TABLE CHP_Desc (
	ID TEXT PRIMARY KEY,
	Description TEXT
);

-- CHP Data
DROP TABLE IF EXISTS CHP_INC CASCADE;
CREATE TABLE CHP_INC (
	ID SERIAL PRIMARY KEY,
	CC_CODE TEXT,
	INC_NUM INTEGER,
	Time Timestamp NOT NULL,
	Description TEXT NOT NULL, --REFERENCES CHP_Desc(ID),
	-- Location DELETE
	-- Area DELETE
	-- Zoom_Map DELETE
	-- TB_XY DELETE
	Latitude FLOAT NOT NULL,
	Longitude FLOAT NOT NULL,
	Location geography(POINT,4326) NOT NULL,
	District_ID INTEGER NOT NULL REFERENCES District(ID),
	CC_ID INTEGER NOT NULL REFERENCES County_City(ID),
	Fwy_ID INTEGER NOT NULL REFERENCES Freeways(ID),
	State_PM FLOAT,
	ABS_PM FLOAT NOT NULL,
	Severity TEXT,
	Duration INTEGER
);

-- CREATE OR REPLACE VIEW CHP_INC_COLLISION AS
--   SELECT *
--   FROM CHP_INC
--   WHERE Desc_ID IN ('1179', '1181', '1182', '1183', '1183H', '20001', '20002');

-- Weather Data
DROP TABLE IF EXISTS Weather_Station CASCADE;
CREATE TABLE Weather_Station (
	ID SERIAL PRIMARY KEY,
	Name TEXT NOT NULL,
	Latitude FLOAT NOT NULL,
	Longitude FLOAT NOT NULL,
	Location geography(POINT,4326) NOT NULL,
	Elevation FLOAT NOT NULL
	--CCID_ID INTEGER NOT NULL REFERENCES County_City(ID)
);

DROP TABLE IF EXISTS Precipitation_Hourly_Observation CASCADE;
CREATE TABLE Precipitation_Hourly_Observation (
	ID SERIAL PRIMARY KEY,
	Station_ID INTEGER NOT NULL REFERENCES Weather_Station(ID),
	End_Hour Timestamp NOT NULL,
	Amount FLOAT NOT NULL
);

DROP TABLE IF EXISTS Precipitation_Daily_Total CASCADE;
CREATE TABLE Precipitation_Daily_Total (
	ID SERIAL PRIMARY KEY,
	Station_ID INTEGER NOT NULL REFERENCES Weather_Station(ID),
	Day Date NOT NULL,
	Amount FLOAT NOT NULL
);

CREATE OR REPLACE FUNCTION LocationTrigger()
RETURNS trigger
AS $loc_upd$
	DECLARE
		ins_txt text;
	BEGIN
		ins_txt := format('SRID=4326;POINT(%s %s)', NEW.Longitude, NEW.Latitude);
		NEW.Location = ST_GeographyFromText(ins_txt);

	RETURN NEW;

	EXCEPTION
	    WHEN data_exception THEN
	        RAISE EXCEPTION 'Trigger ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
	        RETURN NULL;
	    WHEN unique_violation THEN
	        RAISE EXCEPTION 'Trigger ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
	        RETURN NULL;
	    WHEN OTHERS THEN
	        RAISE EXCEPTION 'Trigger ERROR [OTHER] - SQLSTATE: %, SQLERRM: %, (%, %)', SQLSTATE, SQLERRM, NEW.Longitude, NEW.Latitude;
	        RETURN NULL;
END;
$loc_upd$
LANGUAGE plpgsql;

CREATE TRIGGER InsLoc BEFORE INSERT ON Traffic_Station FOR EACH ROW EXECUTE PROCEDURE LocationTrigger();
CREATE TRIGGER InsLoc BEFORE INSERT ON CHP_INC FOR EACH ROW EXECUTE PROCEDURE LocationTrigger();
CREATE TRIGGER InsLoc BEFORE INSERT ON Weather_Station FOR EACH ROW EXECUTE PROCEDURE LocationTrigger();


CREATE OR REPLACE FUNCTION StationIDTrigger()
RETURNS trigger
AS $sid_upd$
	BEGIN
		NEW.Station_ID = (
			SELECT ID
			FROM Traffic_Station ts
			WHERE ts.PEMS_ID = NEW.Station_ID
			AND ((EXTRACT(YEAR FROM Effective_Start) < NEW.Year)
				  OR (EXTRACT(YEAR FROM Effective_Start) = NEW.Year AND EXTRACT(DOY FROM Effective_Start) <= NEW.DOY))
			AND ((NEW.Year = EXTRACT(YEAR FROM Effective_End) AND NEW.DOY < EXTRACT(DOY FROM Effective_End))
				 OR (NEW.Year < EXTRACT(YEAR FROM Effective_End))
				 OR (Effective_End IS NULL))
		);

		IF NEW.Station_ID IS NULL THEN
			NEW.Station_ID = -1;
		END IF;

	RETURN NEW;

	EXCEPTION
	    WHEN data_exception THEN
	        RAISE EXCEPTION 'Trigger ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
	        RETURN NULL;
	    WHEN unique_violation THEN
	        RAISE EXCEPTION 'Trigger ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
	        RETURN NULL;
	    WHEN OTHERS THEN
	        RAISE EXCEPTION 'Trigger ERROR [OTHER] - SQLSTATE: %, SQLERRM: %', SQLSTATE, SQLERRM;
	        RETURN NULL;
END;
$sid_upd$
LANGUAGE plpgsql;

CREATE TRIGGER UpdSID BEFORE INSERT ON Observations FOR EACH ROW EXECUTE PROCEDURE StationIDTrigger();

CREATE OR REPLACE FUNCTION YearDOYToDate(IN Year SMALLINT, IN DOY SMALLINT)
RETURNS Date
AS $$
BEGIN
    RETURN (date (Year || '-01-01') + (interval '1 day'*(DOY-1)))::date;
END;
$$
LANGUAGE plpgsql;
