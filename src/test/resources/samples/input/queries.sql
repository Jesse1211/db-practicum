SELECT S.A FROM Sailors S, Reserves R WHERE S.B = R.G AND R.H < 100 AND S.A >= 9050 ;
SELECT DISTINCt S.A, R.G FROM Sailors S, Reserves R, Boats B WHERE S.B = R.G AND S.A = B.D AND R.H <> B.D AND R.H < 100 ORDER BY S.A;
SELECT * FROM Boats;
SELECT Sailors.A FROM Sailors;
SELECT Boats.F, Boats.D FROM Boats;
SELECT Sailors.A FROM Sailors WHERE Sailors.A < 50;
SELECT * FROM Sailors, Boats WHERE Sailors.A = Boats.D AND Sailors.A < 500 AND Boat.E > 30 ORDER BY Sailors.A, Boats.D;
SELECT * FROM Boats WHERE Boats.E > 20 AND Boats.D < 2000 ORDER BY Boats.E;
SELECT Boats.E FROM Boats WHERE Boats.D < 2000;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
SELECT * FROM Sailors S1, Reserves, Boats WHERE S1.A = Reserves.G AND Reserves.H = Boats.D AND S1.B < 150;
SELECT DISTINCT * FROM Sailors; -- Order Unmatched
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT B.F, B.D FROM Boats B ORDER BY B.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;

-- test for benchmarking
SELECT * FROM Sailors S1, Sailors S2, Sailors S3, Sailors S4, Sailors S5 WHERE S1.A = S2.A AND S2.B = S3.B AND S3.C = S4.C ORDER BY S1.A;
SELECT * FROM Sailors S1, Sailors S2, Sailors S3, Sailors S4, Sailors S5 WHERE S1.A = S2.B AND S2.B = S3.C AND S3.C = S4.A;
SELECT * FROM Sailors S1, Sailors S2, Sailors S3, Sailors S4, Sailors S5;