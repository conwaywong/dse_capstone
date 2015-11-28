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
	ID INTEGER PRIMARY KEY,
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
	ID INTEGER PRIMARY KEY,
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
	ID INTEGER PRIMARY KEY,
	Num INTEGER NOT NULL,
	Direction TEXT NOT NULL
);

CREATE INDEX Fwy_Num_Dir ON Freeways(Num, Direction);

DROP TABLE IF EXISTS County_City CASCADE;
CREATE TABLE County_City (
	ID INTEGER PRIMARY KEY,
	County_FIPS_ID INTEGER NOT NULL,
	City_FIPS_ID INTEGER
);

-- Phase 1
DROP TABLE IF EXISTS Traffic_Station CASCADE;
CREATE TABLE Traffic_Station (
	ID INTEGER PRIMARY KEY,
	PEMS_ID INTEGER,
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
	Length FLOAT,
	Type_ID INTEGER NOT NULL REFERENCES ST_Type(ID),
	Num_Lanes INTEGER NOT NULL
	-- USER_ID DELETE
);

CREATE INDEX Traffic_Station_Idx ON Traffic_Station(ID);

-- Phase 2
DROP TABLE IF EXISTS Observation CASCADE;
CREATE TABLE Observation (
	ID BIGINT PRIMARY KEY,
	Time Timestamp NOT NULL,
	Station_ID INTEGER NOT NULL REFERENCES Traffic_Station(ID),
	Samples INTEGER,
	Perc_Observed FLOAT,
	Total_Flow INTEGER,
	Avg_Occupancy FLOAT,
	Avg_Speed FLOAT
);

-- Phase 3
DROP TABLE IF EXISTS Lane_Observation CASCADE;
CREATE TABLE Lane_Observation (
	Observation_ID BIGINT NOT NULL REFERENCES Observation(ID),
	Station_ID INTEGER NOT NULL REFERENCES Traffic_Station(ID),
	L_Num INTEGER NOT NULL,
	Samples INTEGER,
	Flow INTEGER,
	Occupancy FLOAT,
	Speed FLOAT,
	Obs_Flag SMALLINT
);

CREATE UNIQUE INDEX L_Obso_PriIdx ON Lane_Observation (Observation_ID, Station_ID, L_Num);
CREATE INDEX L_Obos_SecIdx ON Lane_Observation(Station_ID, L_Num);

-- CHP Data
DROP TABLE IF EXISTS CHP_INC CASCADE;
CREATE TABLE CHP_INC (
	ID INTEGER PRIMARY KEY,
	CC_CODE INTEGER,
	INC_NUM INTEGER,
	Time Timestamp NOT NULL,
	Description TEXT,
	-- Location DELETE
	-- Area DELETE
	-- Zoom_Map DELETE
	-- TB_XY DELETE
	Latitude FLOAT NOT NULL,
	Longitude FLOAT NOT NULL,
	District_ID INTEGER NOT NULL REFERENCES District(ID),
	CCID INTEGER NOT NULL REFERENCES County_City(ID),
	Fwy_ID INTEGER NOT NULL REFERENCES Freeways(ID),
	State_PM FLOAT NOT NULL,
	ABS_PM FLOAT NOT NULL,
	Severity INTEGER,
	Duration INTEGER
);

-- Weather Data
DROP TABLE IF EXISTS Weather_Station CASCADE;
CREATE TABLE Weather_Station (
	ID INTEGER PRIMARY KEY,
	Name TEXT NOT NULL,
	Latitude FLOAT NOT NULL,
	Longitude FLOAT NOT NULL,
	Elevation FLOAT NOT NULL
	--CCID_ID INTEGER NOT NULL REFERENCES County_City(ID)
);

DROP TABLE IF EXISTS Precipitation_Hourly_Observation CASCADE;
CREATE TABLE Precipitation_Hourly_Observation (
	ID INTEGER PRIMARY KEY,
	Station_ID INTEGER NOT NULL REFERENCES Weather_Station(ID),
	Time Timestamp NOT NULL,
	Amount FLOAT NOT NULL
);

DROP TABLE IF EXISTS Precipitation_Daily_Total CASCADE;
CREATE TABLE Precipitation_Daily_Total (
	ID INTEGER PRIMARY KEY,
	Station_ID INTEGER NOT NULL REFERENCES Weather_Station(ID),
	Day Date NOT NULL,
	Amount FLOAT NOT NULL
);
