EXPLAIN (ANALYZE, BUFFERS)
SELECT t.ID AS S_ID, t.Num_Lanes, t.Length, t.Urban, t.Density,
       f.Num AS F_NUM, CASE f.Direction WHEN 'N' THEN 0 WHEN 'E' THEN 1 WHEN 'S' THEN 2 WHEN 'W' THEN 3 ELSE -1 END AS F_DIR,
       z.Avg_Value,
       CASE WHEN chp.ID IS NULL THEN 'F' ELSE 'T' END AS CHP_I, chp.CC_CODE, chp.Description, chp.Duration,
       YearDOYToDate(o.Year, o.DOY) AS O_DATE, o.Flow_Coef
FROM Observations o
        INNER JOIN Traffic_Station t ON (o.Station_ID=t.ID)
        INNER JOIN ST_Type st ON (t.Type_ID=st.ID AND st.Type='ML')
        INNER JOIN Freeways f ON (f.ID=t.Fwy_ID)
        LEFT OUTER JOIN Zillo_Home_Value z ON
            ((EXTRACT(YEAR FROM z.Month)=o.Year) AND
             EXTRACT(MONTH FROM z.Month)=EXTRACT(MONTH FROM YearDOYToDate(o.Year, o.DOY)))
        INNER JOIN County_Zip cz ON (t.ZIPCODE=cz.ZIPCODE AND cz.ZIPCODE=z.ZIPCODE)
        LEFT OUTER JOIN CHP_INC chp ON (
            chp.Time::date=YearDOYToDate(o.Year, o.DOY)
            AND chp.Fwy_ID=t.Fwy_ID
            AND ST_Distance(chp.Location, t.Location) < 804.672 -- Half-Mile away
        )
WHERE o.Year=2008
LIMIT 5000000;

-- Select out the Weekend/Day_Coef values
SELECT COUNT(*)
FROM Observations
WHERE EXTRACT(DOW FROM YearDOYToDate(Year, DOY)) NOT IN (0, 6) -- Weekday
-- WHERE EXTRACT(DOW FROM YearDOYToDate(Year, DOY)) IN (0, 6) -- Weekend
AND Weekday_Coef IS NULL;
