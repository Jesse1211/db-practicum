SELECT * FROM Reserves R, Sailors S WHERE R.A < S.B;
SELECT * FROM Reserves R WHERE R.A < R.B;
SELECT * FROM Reserves R, Sailors S WHERE R.A < R.B AND S.C = 1 AND R.D = S.G AND R.D = S.A;
SELECT * FROM R, S, T WHERE R.A = 1 AND R.B = S.C AND T.G < 5 AND T.G = S.H;

SELECT * FROM Sailors;
SELECT Sailors.A FROM Sailors;
SELECT S.A FROM Sailors S;
SELECT * FROM Sailors S WHERE S.A < 3;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT DISTINCT R.G FROM Reserves R;
SELECT * FROM Sailors ORDER BY Sailors.B;