# Notes from P4 instruction:

## How to run
```
\db-practicum> java -jar ./build/libs/db_practicum_early-bird-4.jar ./src/test/resources/samples/interpreter_config_file.txt
```

# Notes from P3 instruction:

## How to run
```
\db-practicum> java -jar ./build/libs/db_practicum_early-bird-3.jar ./src/test/resources/samples/interpreter_config_file.txt
```


# Notes from P2 instruction:

## How to run
```
\db-practicum> java -jar ./build/libs/db_practicum_early-bird-2.jar ./src/test/resources/samples/input ./src/test/resources/samples/expected_output ./src/test/resources/samples/temp
```

# Notes from P1 instruction:

## `inputdir` structure from db_practicum_early-bird-1.jar:
  ```
  \db-practicum> java -jar ./build/libs/db_practicum_early-bird-1.jar ./src/test/resources/samples/input ./src/test/resources/samples/expected_output_1
  15:39:08.801 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors
  15:39:08.808 [main] INFO  compiler.Compiler - Processing query: SELECT Sailors.A FROM Sailors
  15:39:08.809 [main] INFO  compiler.Compiler - Processing query: SELECT S.A FROM Sailors S
  15:39:08.810 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors S WHERE S.A < 3
  15:39:08.817 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G
  15:39:08.820 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A
  15:39:08.824 [main] INFO  compiler.Compiler - Processing query: SELECT DISTINCT R.G FROM Reserves R
  15:39:08.825 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors ORDER BY Sailors.B
  15:39:08.827 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB1 WHERE DB1.B > 25
  15:39:08.828 [main] INFO  compiler.Compiler - Processing query: SELECT DB1.A, DB1.B FROM DB1 WHERE DB1.A < 4
  15:39:08.829 [main] INFO  compiler.Compiler - Processing query: SELECT DB1.A, DB2.H FROM DB1, DB2 WHERE DB1.A = DB2.G
  15:39:08.830 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB2 WHERE DB2.G >= 2
  15:39:08.832 [main] INFO  compiler.Compiler - Processing query: SELECT DISTINCT DB2.G FROM DB2
  15:39:08.833 [main] INFO  compiler.Compiler - Processing query: SELECT S1.A, S2.A FROM DB1 S1, DB1 S2 WHERE S1.A < S2.A
  15:39:08.836 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB1 ORDER BY DB1.B ASC
  15:39:08.837 [main] INFO  compiler.Compiler - Processing query: SELECT S1.A, S2.A, S3.G FROM DB1 S1, DB1 S2, DB2 S3 WHERE S1.A < S2.A AND S1.A = S3.G AND S3.H = 103
  ```
## Strategy from extracting join conditions from `WHERE` and evaluating as part of join:

1. Preparing data:
  - Store all table including aliases into `ArrayList<Table> allTables`
  - Flatten the `WHERE` expressions into `ArrayList<ComparisonOperator> flattened`. Nested comparisons will be extracted. This works because we only consider `AND` operators.
  - Initialize expressions `Expression joinWhereExpression`, `Expression valueWhereExpression`, and `Map<String, Expression> tableWhereExpressionMap`.
2. Categorize elements `comparisonOperator` inside `flattened` by 3 conditions and put into corresponding container:
  - Identify as value comparison (42 = 42): element has no left table and right table name.
  - Identify as same-table comparison (s.a = 42, s.a = s.b): either left or right expression is a value, or both left and right column have the same table name or alias
  - Identify as cross-table comparison (s1.a = s2.a): left and right are not the same table

3. Scan each table and select the tuples based expression in `tableWhereExpressionMap`, store the operators in a queue.

4. Use `JoinOperators` to join all operators in the queue. This will result one final operator.
5. Use SelectOperators to evaluate `valueWhereExpression` and `joinWhereExpression`
