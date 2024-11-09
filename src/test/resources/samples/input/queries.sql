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