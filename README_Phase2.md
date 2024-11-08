# Design Architecture (Phase 2 final - Visitor Pattern)

### The top-level class is the `Compiler` class located in `/src/main/java/compiler/Compiler.java`

## File locations

- ### Logical operators are located at `/src/main/java/operator_node/` folder
- ### Physical operators are located at `/src/main/java/physical_operator` folder
- ### Physical plan builder is located at `/src/main/java/common/PhysicalPlanBuilder`
- ### Logical plan builder is located at `/src/main/java/common/LogicalPlanBuilder`

## Logical Plan

Construct a `Tree` by `OperatorNode` with relational algebra intuition, prepare to be traversed by Physical Plan

- Parse data by necessary, and build corresponding OperatorNode
- `outputSchema`: set selected column from table
- `childNode`: the child node of each operator
- `parentNode`: the parent node of each operator
- `accept`: Visit the current class by the `operatorNodeVisitor`, next step should accept the child's node in

### `OperatorNode` types

- `DuplicateEliminationOperatorNode` for query `DISTINCT`, has child & parent nodes
- `EmptyOperatorNode` for empty result, does not has child & parent nodes (LEAF)
- `JoinOperatorNode` for table join, has left & right child & parent nodes
- `ProjectOperatorNode` for projection, has child & parent nodes
- `ScanOperatorNode` for scan, does not have child & parent nodes (LEAF)
- `SelectOperatorNode` for select, has child & parent nodes
- `SortOperatorNode` for sort, has child & parent nodes

## Physical Plan

- `visit`: Takes the `OperatorNode` from Logical plan and create `Operator` for data processing
  - Utilized overloading to handle different `OperatorNode` types
  - Create corresponding `Operator` types based on input tupes
- `getResult`: return the `first` operator to be processed, for recursive processing purpose

### `OperatorNode` types

#### `ScanOperator(table)`: **Scan all info from the Table by Buffer Reader**

- `Outputschema`: Set as all columns from table
- Initialize buffer reader from file based on `tableName`
- `reset()`: re-initialize buffer reader
- `getNextTuple()`: read and return the row as tuple

#### `SelectOperator(childOperator, whereExpression)`: **Process, Filter all comparators from where**

- `outputSchema`: no needed to modify
- Use childOperator's outputSchema to a map as `{columnName: index in outputSchema}`
- Use the map and data rows from `childOperator` for `ExpressionEvaluator`
- `reset()`: reset it's childOperator -> reset buffer reader
- `getNextTuple()`: return satisfied row as tuple based on `ExpressionEvaluator`

#### `ProjectOprtator(childOperator, selectedItems)` **Determine output format from selectedItems**

- `outputSchema`: update by `selectedItems` - after SELECT before WHERE
  - outputSchema had ALL columns before, now only store what is mentioned from selectedItems
- Use childOperator's outputSchema to a map as `{columnName: index in outputSchema}`
- `reset()`: call childOperator's reset()
- `getNextTuple()`: based on filtered output, return only selected column as tuple

#### `DuplicateEliminationOperator(childOperator)`: **Remove duplicates**

- Initialize a set for duplication check from calling `childOperator's getNextTuple()`

#### `SortOperator(childOperator, elementOrders)`: **In memory sort**

- Fetch all values from `childOperator.getNextTuple()`
- Implement a sort function and list iterator
- `reset()`: reset iterator
- `getNextTuple()`: trigger iterator

#### `ExternalSortOperator(outputSchema, childOperator, orderColumns, bufferSize)`: **External Sort**

- `divideAndSort()`:Load buffer size of tuples, then sort and store to a File named with \_UUID.temp
- `mergedFile = mergeSortedFiles()`: Utilize Priority Queue to find 'best' tuple among all files, then write to ONE merged file
- delete files on exiting the temp files
- `reset()`: reset iterator
- `getNextTuple()`: Find next tuple from mergedFile by using BinaryHandler

#### `JoinOperator(outputSchema, leftOperator, rightOperator)`: **TNLJ**

- Fetch one tuple from both operators, return the gluded tuple

#### `BNLJOperator(outputSchema, leftOperator, rightOperator, bufferSize)`: **BNLJ**

- `leftTupleBlockIndex`: used to keep in track of last visited tuple from left child block
- `loadLeftChildBlock()`: Load ONE left child block into memory, this is being called once all tuples in block is traversed
- `getNextTuple()`: Traverse block from updated leftTupleBlockIndex. If traversed all blocks, jump to next tuple from right child.

#### `SMJOperator(outputSchema, leftOperator, rightOperator, leftColumn, rightColumn)`: **SMJ**

- Restriction: check if join has at least equality condition
- Get equality condition, extract left and right columns as `leftSortOperator` and `rightSortOperator`
- Use `leftColumnMap` and `rightColumnMap` to keep track of the value and index for index resting purpose
- `getNextTuple()`: Find the match tuples by comparing the values in columns.
  - if tuples have same value:
    - Add tuple and value into map for record if map doesn't have
  - if tuples value are different
    - load next tuple, and check and reset the index from map to solve duplication values

### config.properties

**We are expecting the input format is alway correct.**

#### first line - Join algorithms:

- `0`: TNLJ
- `1 x`: BNLJ with buffer size as x page
- `2`: SMJ

#### second line - sort algorithms:

- `0`: In-Memory Sort
- `1 x`: External Sort with buffer size as x page

---

---

---

---

# Design Architecture (Phase 1)

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
