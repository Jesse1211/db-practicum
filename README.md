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
