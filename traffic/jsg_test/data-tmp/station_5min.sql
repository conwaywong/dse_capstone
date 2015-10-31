
/* Origional Fields
Timestamp

Station
District
Freeway #
Direction of Travel
Lane Type
Station Length

Samples
% Observed
Total Flow
Avg Occupancy
Avg Speed

Lane N Samples
Lane N Flow
Lane N Avg Occ
Lane N Avg Speed
Lane N Observed
*/

CREATE TABLE Station (
	Station_ID INTEGER,
	District INTEGER,
	Freeway_NO, INTEGER,
	Direction of Travel CHAR(1), -- N,S,E,W
	Lane Type CHAR(2), -- CD, CH, FF, FR, HV, ML, OR
	Station Length NUMERIC
);

CREATE TABLE Observation (
	ID INTEGER,
	Time TIMESTAMP,
	Station_ID NOT NULL REFERENCES Station(Station_ID),
	Samples INTEGER, 
	Perc_Observed NUMERIC,
	Total_Flow NUMERIC,
	Avg_Occupancy NUMERIC,
	Avg_Speed NUMERIC
);

CREATE TABLE Lanes (
	Observation_ID INTEGER REFERENCES Observation(ID),
	Samples INTEGER,
	FLow INTEGER,
	Avg_Occ NUMERIC,
	Avg_Speed NUMERIC,
	Observed INTEGER
);

