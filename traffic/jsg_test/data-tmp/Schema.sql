-- PEMS Schema

CREATE TABLE District (
	ID INTEGER,
	NAME TEXT
);

CREATE TABLE Freeways (
	ID INTEGER,
	NUM INTEGER,
	DIR TEXT
);

CREATE TABLE ST_Type (
	ID INTEGER,
	Type TEXT
);

CREATE TABLE County_City (
	ID INTEGER,
	County_FIPS_ID INTEGER,
	City_FIPS_ID INTEGER
);

CREATE TABLE Station (
	ID INTEGER,
	Fwy_ID INTEGER,
	CCID ID INTEGER,
	District_ID INTEGER,
	State_PM FLOAT,
	ABS_PM FLOAT,
	Latitude FLOAT,
	Longitude FLOAT,
	Length FLOAT,
	Type_ID INTEGER,
	Num_Lanes INTEGER,
	-- USER_ID DELETE
);

CREATE TABLE Observation (
	ID INTEGER,
	Station_ID INTEGER,
	Time Timestamp,
	Samples INTEGER,
	Perc_Observed FLOAT,
	Total_Flow INTEGER,
	Avg_Occ FLOAT,
	Avg_Speed FLOAT
);

CREATE TABLE CHP_INC (
	ID INTEGER,
	CC_CODE INTEGER,
	INC_NUM INTEGER,
	Time Timestamp,
	Description TEXT,
	-- Location DELETE
	-- Area DELETE
	-- Zoom_Map DELETE
	-- TB_XY DELETE
	Latitude FLOAT,
	Longitude FLOAT,
	District_ID INTEGER,
	CCID INTEGER,
	Fwy_ID INTEGER,
	State_PM FLOAT,
	ABS_PM FLOAT,
	Severity INTEGER,
	Duration INTEGER
);