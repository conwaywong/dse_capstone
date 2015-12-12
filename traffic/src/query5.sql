-- Flow:
SELECT s.Name, s.ABS_PM,-- date_trunc('hour', o.Time) AS hour,
       date_trunc('hour', o.Time) + INTERVAL '30 min' * ROUND(date_part('minute', o.Time) / 30.0) AS T_Int,
       avg(o.Total_Flow) AS Int_Avg_F,
       avg(o.Avg_Speed) AS Int_Avg_S
  FROM Traffic_Station s
            LEFT OUTER JOIN Freeways f ON (f.ID=s.Fwy_ID)
            LEFT OUTER JOIN ST_Type t ON (t.ID=s.Type_ID),
       Observation o
 WHERE s.ID=o.Station_ID
   AND f.Num=52
   AND f.Direction='E'
   AND o.TIME BETWEEN '2010-01-13' AND '2010-01-14'
   AND t.Type='ML'
GROUP BY 1,2,3
HAVING avg(o.Avg_Speed) < 45
ORDER BY 3, s.ABS_PM;


SELECT DISTINCT f.Num, f.Direction, FROM Avg_30Min.t_30
FROM Freeways f
        LEFT OUTER JOIN Traffic_Station s on (f.ID=s.Fwy_ID),
    -- Calculate station avg Speed for a Day
    (SELECT s.ID, date_trunc('day', o.Time), avg(o.Avg_Speed)
    FROM Observation o
            LEFT OUTER JOIN Traffic_Station s ON (s.ID=o.Station_ID)
    GROUP BY 1,2) Avg_day(id, d, a_d),
    -- Find 30 minute interval average speed
    (SELECT s.ID,
           date_trunc('hour', o.Time) + INTERVAL '30 min' * ROUND(date_part('minute', o.Time) / 30.0),
           avg(o.Avg_Speed)
    FROM Observation o
           LEFT OUTER JOIN Traffic_Station s ON (s.ID=o.Station_ID)
    GROUP BY 1,2) Avg_30Min(id, t_30, a_30m)
WHERE Avg_day.id = Avg_30Min.id
-- 30min period where avgspeed is 40% of that days average
AND Avg_30Min.a_30m < (Avg_day.a_d*0.4)
AND date_trunc('day', Avg_30Min.t_30) = Avg_day.d
AND Avg_day.id=s.ID
ORDER BY 1,2,3;

-- Traffic stations that have similar avg_speed for a given 15 minute window
CREATE OR REPLACE FUNCTION round_15m(TIMESTAMP WITH TIME ZONE)
RETURNS TIMESTAMP WITH TIME ZONE AS $$
  SELECT date_trunc('hour', $1) + INTERVAL '15 min' *
         ROUND(date_part('minute', $1) / 15.0)
$$ LANGUAGE SQL;

WITH Avg_15min(id, t_15, a_15m)
AS
(
    SELECT T.id, T.t_15, avg(T.avg_s)
    FROM (SELECT s.ID,
                 round_15m(o1.Time),
                 o1.Avg_Speed
          FROM Observation o1
                LEFT OUTER JOIN Traffic_Station s ON (s.ID=o1.Station_ID)
          WHERE o1.Samples <> 0) T(id, t_15, avg_s)
    GROUP BY 1,2
    HAVING avg(T.avg_s) < (SELECT avg(o2.avg_speed)*0.4
                             FROM Observation o2
                            WHERE o2.Station_ID=T.id
                              AND date_trunc('day', T.t_15) = date_trunc('day', o2.Time))
)
SELECT DISTINCT f1.Num, f1.Direction, f2.Num, f2.Direction
FROM Traffic_Station s1
        LEFT OUTER JOIN Avg_15min a1 ON (a1.id=s1.ID)
        LEFT OUTER JOIN Freeways f1 ON (f1.ID=s1.Fwy_ID),
     Traffic_Station s2
        LEFT OUTER JOIN Avg_15min a2 ON(a2.id=s2.ID)
        LEFT OUTER JOIN Freeways f2 ON (f2.ID=s2.Fwy_ID)
WHERE a1.t_15 = a2.t_15
AND a2.a_15m <= (a1.a_15m - (a1.a_15m*0.1))
AND (a1.a_15m + (a1.a_15m*0.1)) <= a2.a_15m;

Cost: 3,456,120,028,179.20

WITH Avg_15min(id, t_15, FID, a_15m)
AS
(
    SELECT T.id, T.t_15, t.FID, avg(T.avg_s)
    FROM (SELECT s.ID,
                 round_15m(o1.Time),
                 s.Fwy_ID,
                 o1.Avg_Speed
          FROM Traffic_Station s
                LEFT OUTER JOIN Observation o1 ON (s.ID=o1.Station_ID)
                LEFT OUTER JOIN ST_Type t ON (s.Type_ID=t.ID)
                LEFT OUTER JOIN Freeways f ON (s.Fwy_ID=f.ID)
          WHERE t.Type = 'ML'
            AND f.Num IN (5, 805, 52)
            AND o1.Samples <> 0) T(id, t_15, FID, avg_s)
    GROUP BY 1,2,3
    HAVING avg(T.avg_s) < (SELECT avg(o2.avg_speed)*0.4
                             FROM Observation o2
                            WHERE o2.Station_ID=T.id
                              AND date_trunc('day', T.t_15) = date_trunc('day', o2.Time))
)
SELECT DISTINCT f1.Num, f1.Direction, f2.Num, f2.Direction
FROM Avg_15min a1
        LEFT OUTER JOIN Freeways f1 ON (f1.ID=a1.FID),
     Avg_15min a2
        LEFT OUTER JOIN Freeways f2 ON (f2.ID=a2.FID)
WHERE a1.t_15 = a2.t_15
AND a2.a_15m <= (a1.a_15m - (a1.a_15m*0.1))
AND (a1.a_15m + (a1.a_15m*0.1)) <= a2.a_15m;

Cost: 461,180,743,432.26
Cost: 120,308,265,841.43
