# Notes from P1 instruction:

- `inputdir` structure from db_practicum_early-bird-1.jar:
  ```
  jesseliu@dhcp-vl2041-18670 libs % java -jar db_practicum_early-bird-1.jar input expected_output
  17:35:18.727 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors
  17:35:18.730 [main] INFO  compiler.Compiler - Processing query: SELECT Sailors.A FROM Sailors
  17:35:18.730 [main] INFO  compiler.Compiler - Processing query: SELECT S.A FROM Sailors S
  17:35:18.731 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors S WHERE S.A < 3
  17:35:18.733 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G
  17:35:18.734 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A
  17:35:18.735 [main] INFO  compiler.Compiler - Processing query: SELECT DISTINCT R.G FROM Reserves R
  17:35:18.736 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM Sailors ORDER BY Sailors.B
  17:35:18.736 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB1 WHERE DB1.B > 25
  17:35:18.737 [main] INFO  compiler.Compiler - Processing query: SELECT DB1.A, DB1.B FROM DB1 WHERE DB1.A < 4
  17:35:18.737 [main] INFO  compiler.Compiler - Processing query: SELECT DB1.A, DB2.H FROM DB1, DB2 WHERE DB1.A = DB2.G
  17:35:18.738 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB2 WHERE DB2.G >= 2
  17:35:18.738 [main] INFO  compiler.Compiler - Processing query: SELECT DISTINCT DB2.G FROM DB2
  17:35:18.739 [main] INFO  compiler.Compiler - Processing query: SELECT S1.A, S2.A FROM DB1 S1, DB1 S2 WHERE S1.A < S2.A
  17:35:18.739 [main] INFO  compiler.Compiler - Processing query: SELECT * FROM DB1 ORDER BY DB1.B ASC
  ```
- Strategy from extracting join conditions from `WHERE` and evaluating as part of join:

  1. Preparing data:

  - - Store all table names including aliases into `ArrayList<Table> allTables`
  - - Flatten the `WHERE` expressions into `ArrayList<ComparisonOperator> flattened` because we only consider AND operators.
  - - Initialize variables, prepare for categorizing data into join expressions - `Expression joinWhereExpression` and value expressions - `Expression valueWhereExpression`, and map from table name to expression - `Map<String, Expression> tableWhereExpressionMap`

  2. Categorize elements `comparisonOperator` inside `flattened` by 3 conditions:

  - - Identify as value comparison: element has no left table or right table name. Then bind to `valueWhereExpression` by `AndExpression`
  - - Identify as table comparasion without join: at lease one table is null or both tables are same. Update the corresponded AndExpression from `tableWhereExpressionMap`
  - - join tables other wise. Then bind to `valueWhereExpression` by `AndExpression`

  3. Scan and Select each table from `allTables` by offering to queue
  4. Keep polling queue and concatenate each operators until 1 item left:

  - - At each iteration, poll twice to get left & right child operators, then concatenate into a Join Operator. Offer the concatenated operator into queue.

  5. Identify the final operator by detecting as value / join comparison

# Design Architecture

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
- `reset()`: no needed to modify
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
