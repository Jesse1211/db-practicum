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

## Design Architecture

#### 1. **Parse** data by necessary

#### 2. **Build table plan from Join & FromItems** as `List<Table>` to collect all tables needed

#### 3. **Scan all info from the Table by Buffer Reader** in `ScanOperator(table)`

- `Outputschema`: Set as all columns from table
- Initialize buffer reader from file based on `tableName`
- `reset()`: re-initialize buffer reader
- `getNextTuple()`: read and return the row as tuple

#### 4. **Process, Filter all comparators from where** in `SelectOperator(childOperator, whereExpression)`

- `outputSchema`: no needed to modify
- Use childOperator's outputSchema to a map as `{columnName: index in outputSchema}`
- Use the map and data rows from `childOperator` for `ExpressionEvaluator`
- `reset()`: reset it's childOperator -> reset buffer reader
- `getNextTuple()`: return satisfied row as tuple based on `ExpressionEvaluator`

#### 5. **Determine output format from selectedItems** in `ProjectOprtator(childOperator, selectedItems)`

- `outputSchema`: update by `selectedItems` - after SELECT before WHERE
  - outputSchema had ALL columns before, now only store what is mentioned from selectedItems
- Use childOperator's outputSchema to a map as `{columnName: index in outputSchema}`
- `reset()`: call childOperator's reset()
- `getNextTuple()`: based on filtered output, return only selected column as tuple

#### 6. **Order the output** in `SortOperator(childOperator, elementOrders)`

- Fetch all values from `childOperator.getNextTuple()`
- Implement a sort function and list iterator
- `reset()`: reset iterator
- `getNextTuple()`: trigger iterator

#### 7. **Remove duplicates** in `DuplicateEliminationOperator(childOperator)`

- Initialize a set for duplication check from calling `childOperator's getNextTuple()`

# Env Set up

## Java jdk 21 env set on MAC

1. download from online to get a jdk file
2. Move to javahome `sudo mv ~/Downloads/jdk-21.jdk /Library/Java/JavaVirtualMachines/`
3. set as default

```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
```

4. apply changes `source ~/.zshrc`
5. check `java -version`

## Set JAVA_HOME var in env on MAC

1. check version `/usr/libexec/java_home -v 21`
2. set path `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`
3. add path `echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 21)' >> ~/.zshrc`
4. `source ~/.zshrc`

## Don't forget bump gradle version to >= 8.5

`distributionUrl=https\://services.gradle.org/distributions/gradle-8.5-bin.zip`

## Build Instructions

You can build the jars for your deliverable using the following command:

`$ ./gradlew build`

If that fails because of formatting issues, check the code formatting instructions below.

Make sure that your code runs by executing the following:

`java -jar ./build/libs/db_practicum_skeleton-1.0-SNAPSHOT.jar`

Unit tests run automatically when `./gradlew build`. If the tests cannot pass, the script will not produce
the deliverable jar.

**If you try to run the `./gradlew build` command before implementing the deliverable, the command will fail because
of the unit tests. This is completely normal, as you need to implement the required functionality to make the
unit tests pass.**

## Code Formatting Instructions

We use the Google Java Format plugin in order to make sure that we keep a constant code formatting across assignments
of all teams.

### Check Formatting

To check if your code style complies, type the following:

`$ ./gradlew verGJF`

### Fix Format Issues

In case the previous step fails, you can auto-format your code as follows:

`$ ./gradlew goJF`
