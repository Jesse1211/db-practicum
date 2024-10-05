-- select [0, 2]
SELECT * FROM Sailors;
SELECT * FROM Boats;
SELECT * FROM Reserves;

-- scan [3, 23]
SELECT * FROM Sailors where Sailors.A < Sailors.B;
SELECT * FROM Sailors where Sailors.A > Sailors.B;
SELECT * FROM Sailors where Sailors.A = Sailors.B;
SELECT * FROM Sailors where Sailors.A <= Sailors.B;
SELECT * FROM Sailors where Sailors.A >= Sailors.B;
SELECT * FROM Sailors where Sailors.A <> Sailors.B;
SELECT * FROM Sailors where 1=1 AND 1=1;
SELECT * FROM Sailors where 1=1 AND 1=0;
SELECT * FROM Sailors where 1=0 AND 1=0;
SELECT * FROM Sailors where 1<2;
SELECT * FROM Sailors where 3<2;
SELECT * FROM Sailors where 1>2;
SELECT * FROM Sailors where 3>2;
SELECT * FROM Sailors where 1<=2;
SELECT * FROM Sailors where 3<=2;
SELECT * FROM Sailors where 1>=2;
SELECT * FROM Sailors where 3>=2;
SELECT * FROM Sailors where 1=2;
SELECT * FROM Sailors where 1=1;
SELECT * FROM Sailors where 1<>2;
SELECT * FROM Sailors where 1<>1;

-- project [24, 27]
SELECT Sailors.A FROM Sailors;
SELECT Sailors.B FROM Sailors;
SELECT Sailors.A, Sailors.B FROM Sailors;
SELECT Sailors.A, Sailors.B, Sailors.C FROM Sailors;

-- sort [28, 30]
SELECT * FROM Sailors ORDER BY Sailors.A;
SELECT * FROM Sailors ORDER BY Sailors.A, Sailors.B;
SELECT * FROM Sailors ORDER BY Sailors.A, Sailors.B, Sailors.C;

-- duplicate elimination [31]
SELECT DISTINCT Sailors.A FROM Sailors;

-- join [32, 37]
select * FROM Sailors, Boats;
select * FROM Sailors, Sailors;
select * FROM Reserves, Sailors, Boats WHERE Reserves.G = 20 AND Sailors.A = 20;
select * FROM Reserves, Sailors, Boats WHERE Reserves.G = 20 AND Sailors.A = 20 AND Boats.D < 2 AND Boats.F = 99;
select * FROM Sailors AS S1, Sailors AS S2 WHERE S1.A <> S2.A;
select * FROM Sailors AS S1, Sailors AS S2 WHERE S1.A = S2.A;