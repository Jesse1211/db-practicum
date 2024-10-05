-- select [0, 2]
SELECT * FROM Sailors;
SELECT * FROM Boats;
SELECT * FROM Reserves;

-- scan [3, 11]
SELECT * FROM Sailors where Sailors.A < Sailors.B;
SELECT * FROM Sailors where Sailors.A > Sailors.B;
SELECT * FROM Sailors where Sailors.A = Sailors.B;
SELECT * FROM Sailors where Sailors.A <= Sailors.B;
SELECT * FROM Sailors where Sailors.A >= Sailors.B;
SELECT * FROM Sailors where Sailors.A <> Sailors.B;
SELECT * FROM Sailors where 1=1 AND 1=1;
SELECT * FROM Sailors where 1=1 AND 1=0;
SELECT * FROM Sailors where 1=0 AND 1=0;

-- project [12, 15]
SELECT Sailors.A FROM Sailors;
SELECT Sailors.B FROM Sailors;
SELECT Sailors.A, Sailors.B FROM Sailors;
SELECT Sailors.A, Sailors.B, Sailors.C FROM Sailors;

-- sort [16, 18]
SELECT * FROM Sailors ORDER BY Sailors.A;
SELECT * FROM Sailors ORDER BY Sailors.A, Sailors.B;
SELECT * FROM Sailors ORDER BY Sailors.A, Sailors.B, Sailors.C;

-- duplicate elimination [19]
SELECT DISTINCT Sailors.A FROM Sailors;

-- join [20, 23]
select * FROM Sailors, Boats;
select * FROM Sailors, Sailors;
select * FROM Sailors, Boats, Reserves;
select * FROM Reserves, Sailors, Boats;
