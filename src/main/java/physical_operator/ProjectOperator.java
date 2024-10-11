package physical_operator;

import common.HelperMethods;
import common.Tuple;
import java.util.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * An operator for SELECT, only returns the columns stated in the select
 * expression. It will also
 * update the output schema based on the select expression.
 */
public class ProjectOperator extends Operator {
  private Operator childOperator;
  private List<SelectItem> selectItems;
  private Map<String, Integer> columnIndexMap;

  /**
   * ProjectOperator Constructor
   *
   * @param childOperator select | scan operator
   */
  public ProjectOperator(ArrayList<Column> outputSchema, Operator childOperator) {
    super(outputSchema);
    this.childOperator = childOperator;
    this.columnIndexMap = HelperMethods.mapColumnIndex(childOperator.getOutputSchema());
  }

  @Override
  public void reset() {
    childOperator.reset();
  }

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
}
