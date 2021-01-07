-- This query merges Green and Yellow with the gdataspeedtimejan
-- and ydataspeedtimejan

USE FinalProject;
CREATE VIEW GreenAndYellowTaxiViewJan AS
SELECT A.Trip_time_in_minutes ,B.Trip_time_in_minutes AS Second_trip_time, A.trip_distance, B.Trip_distance AS Second_trip_distance, A.Trip_speed, B.Trip_speed AS Second_speed ,
	   A.passenger_count, B.Passenger_count AS Second_trip_passenger, A.tpep_pickup_datetime, B.lpep_pickup_datetime AS Second_pickup, A.tpep_dropoff_datetime, B.Lpep_dropoff_datetime AS Second_dropoff,
       (A.Trip_speed + B.Trip_speed)/2 AS Average_speed,
(ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.Pickup_longitude, B.Pickup_latitude)
 ) * .000621371192) / ((A.Trip_speed + B.Trip_speed)/2) * 60 AS Time_o1_to_o2,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.Dropoff_longitude, B.Dropoff_latitude)
 ) * .000621371192 AS o1_to_d2Dist,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(A.dropoff_Longitutde, A.dropoff_Latitude)
 ) * .000621371192 AS o1_to_d1Dist,
 ST_Distance_Sphere( 
 point(A.pickup_longitude, A.pickup_latitude),
 point(B.Pickup_longitude, B.Pickup_latitude)
 ) * .000621371192 AS o1_to_o2Dist,
 ST_Distance_Sphere( 
 point(B.Pickup_longitude, B.Pickup_latitude),
 point(A.pickup_longitude, A.pickup_latitude)
 ) * .000621371192 AS o2_to_o1Dist,
 ST_Distance_Sphere( 
 point(B.Pickup_longitude, B.Pickup_latitude),
 point(A.dropoff_Longitutde, A.dropoff_Latitude)
 ) * .000621371192 AS o2_to_d1Dist,
 ST_Distance_Sphere( 
 point(B.Pickup_longitude, B.Pickup_latitude),
 point(B.Dropoff_longitude, B.Dropoff_latitude)
 ) * .000621371192 AS o2_to_d2Dist,
 ST_Distance_Sphere( 
 point(A.dropoff_Longitutde, A.dropoff_Latitude),
 point(B.Dropoff_longitude, B.Dropoff_latitude)
 ) * .000621371192 AS d1_to_d2Dist,
 ST_Distance_Sphere( 
 point(B.Dropoff_longitude, B.Dropoff_latitude),
 point(A.dropoff_Longitutde, A.dropoff_Latitude)
 ) * .000621371192 AS d2_to_d1Dist
FROM gdataspeedtimejan AS B
	CROSS JOIN
    ydataspeedtimejan AS A
WHERE B.Passenger_count + A.passenger_count < 4;

-- *************************************************************
-- THis query runs the next condition on the newly create view
-- *************************************************************

USE FinalProject;
CREATE VIEW GreenANDYellowImportantView AS
SELECT *, (o1_to_d2Dist / Average_speed) * 60 AS Time_o1_to_d2,
	 (o1_to_d1Dist / Average_speed) * 60 AS Time_o1_to_d1,
     (o2_to_o1Dist / Average_speed) * 60 AS Time_o2_to_o1,
     (o2_to_d1Dist / Average_speed) * 60 AS Time_o2_to_d1,
     (o2_to_d2Dist / Average_speed) * 60 AS Time_o2_to_d2,
     (d1_to_d2Dist / Average_speed) * 60 AS Time_d1_to_d2,
     (d2_to_d1Dist / Average_speed) * 60 AS Time_d2_to_d1,
     (o1_to_o2Dist+o2_to_d1Dist+d1_to_d2Dist) AS Sequence1Dist,
     (o1_to_o2Dist+ o2_to_d2Dist + d2_to_d1Dist) AS Sequence2Dist,
     (o2_to_o1Dist + o1_to_d1Dist + d1_to_d2Dist) AS Sequence3Dist,
     (o2_to_o1Dist + o1_to_d2Dist + d2_to_d1Dist) AS Sequence4Dist,
     (o1_to_d1Dist + o2_to_d2Dist) AS totalDistanceP2P
FROM greenandyellowtaxiviewjan;

-- *************************************************************
-- This query is the last one we did for the 4 sequences but now
-- with green and yellow merged with each other
-- *************************************************************

-- green data..filter by second pick up time being less than o1 pick up time
USE FinalProject;
CREATE VIEW FinalSequence1FirstLessThanSecond As
 SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, ((Time_o1_to_o2 + Time_o2_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) AS checkSequence1Timing
 FROM greenandyellowimportantview
 WHERE tpep_pickup_datetime < Second_pickup 
 AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist 
 OR totalDistanceP2P > Sequence4Dist) 
 AND ((Time_o1_to_o2 + Time_o2_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) <= 5;

-- **********************************************************
USE FinalProject;
CREATE VIEW FinalSequence2FirstLessThanSecond As
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, ((Time_o1_to_o2 + Time_o2_to_d2 + Time_d1_to_d2) - Time_o1_to_d1) AS checkSequence2Timing
FROM greenandyellowimportantview
WHERE tpep_pickup_datetime < Second_pickup 
AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist 
OR totalDistanceP2P > Sequence4Dist) 
AND ((Time_o1_to_o2 + Time_o2_to_d2 + Time_d1_to_d2) - Time_o1_to_d1) <= 5;

-- ***********************************************************
USE FinalProject;
CREATE VIEW FinalSequence3SecondLessThanFirst As
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, ((Time_o2_to_o1 + Time_o1_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) AS checkSequence3Timing
FROM greenandyellowimportantview
WHERE Second_pickup < tpep_pickup_datetime 
AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist 
OR totalDistanceP2P > Sequence4Dist) 
AND ((Time_o2_to_o1 + Time_o1_to_d1 + Time_d1_to_d2) - Time_o2_to_d2) <= 5;

-- *************************************************************
USE FinalProject;
CREATE VIEW FinalSequence4SecondLessThanFirstGreenJan As
SELECT Sequence1Dist, Sequence2Dist, Sequence3Dist, Sequence4Dist, totalDistanceP2P, ((Time_o2_to_o1 + Time_o1_to_d2 + Time_d2_to_d1) - Time_o1_to_d1) AS checkSequence4Timing
FROM greenandyellowimportantview
WHERE Second_pickup < tpep_pickup_datetime 
AND (totalDistanceP2P > Sequence1Dist OR totalDistanceP2P > Sequence2Dist OR totalDistanceP2P > Sequence3Dist 
OR totalDistanceP2P > Sequence4Dist) 
AND ((Time_o2_to_o1 + Time_o1_to_d2 + Time_d2_to_d1) - Time_o1_to_d1) <= 5;