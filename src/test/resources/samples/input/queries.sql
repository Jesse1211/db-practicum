-- -- select
-- SELECT * FROM Sailors_humanreadable;
-- SELECT * FROM Boats_humanreadable;
-- SELECT * FROM Reserves_humanreadable;

-- -- scan
-- SELECT * FROM Sailors_humanreadable where Sailors_humanreadable.A < Sailors_humanreadable.B;
-- SELECT * FROM Sailors_humanreadable where Sailors_humanreadable.A > Sailors_humanreadable.B;
-- SELECT * FROM Boats_humanreadable;
-- SELECT * FROM Reserves_humanreadable;

-- select
SELECT * FROM Sailors;
SELECT * FROM Boats;
SELECT * FROM Reserves;

-- scan
SELECT * FROM Sailors where Sailors_humanreadable.A < Sailors_humanreadable.B;
SELECT * FROM Sailors where Sailors_humanreadable.A > Sailors_humanreadable.B;
