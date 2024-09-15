package operator;

import common.HelperMethods;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * An operator for SELECT, only returns the columns stated in the select expression. It will also
 * update the output schema based on the select expression.
 */
public class ProjectOperator extends Operator {
  private Operator childOperator;
  private List<SelectItem> selectItems;
  private Map<String, Integer> columnIndexMap;

  /**
   * Constructor
   *
   * @param childOperator select | scan operator
   * @param selectItems list of SELECT as `Table.column1, Table.column2` expression
   */
  public ProjectOperator(Operator childOperator, List<SelectItem> selectItems) {
    super(new ArrayList<>());
    this.childOperator = childOperator;
    this.selectItems = selectItems;
    this.columnIndexMap = HelperMethods.mapColumnIndex(childOperator.getOutputSchema());
    updateOutputSchema();
  }

  /** Invoke childOperator's reset method */
  @Override
  public void reset() {
    childOperator.reset();
  }

  /**
   * @return only returns tuples with selected column based on outputSchema
   */
  @Override
  public Tuple getNextTuple() {
    Tuple tuple;

    if ((tuple = childOperator.getNextTuple()) != null) {
      ArrayList<Integer> tupleArray = new ArrayList<>();

      for (Column column : outputSchema) {
        // Find the index of the column in the output schema
        int index = columnIndexMap.get(column.getName(true));

        // Add the value from the child tuple's data to the new tuple's data array
        tupleArray.add(tuple.getElementAtIndex(index));
      }
      return new Tuple(tupleArray);
    }
    return null;
  }

  /** updates the output schema based on select items from the select statement. */
  private void updateOutputSchema() {
    ArrayList<Column> outputSchema = new ArrayList<>();
    for (SelectItem item : selectItems) {
      if (item instanceof SelectExpressionItem) {
        Column column = (Column) ((SelectExpressionItem) item).getExpression();
        int index = columnIndexMap.get(column.getName(true));
        outputSchema.add(childOperator.getOutputSchema().get(index));
      } else if (item instanceof AllColumns) {
        outputSchema.addAll(this.outputSchema);
      }
    }
    this.outputSchema = outputSchema;
  }
}
