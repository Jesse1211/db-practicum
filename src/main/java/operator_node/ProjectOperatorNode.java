package operator_node;

import common.HelperMethods;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * ProjectOperatorNode is a class to represent the project operator nodes in the logical query plan.
 */
public class ProjectOperatorNode extends OperatorNode {

  private final Map<String, Integer> columnIndexMap;
  private List<SelectItem> selectItems;

  /**
   * Set the node as the child to project operator
   *
   * @param childNode the child node of the project operator
   * @param selectItems the items to be projected from the select statement
   */
  public ProjectOperatorNode(OperatorNode childNode, List<SelectItem> selectItems) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.selectItems = selectItems;
    this.columnIndexMap = HelperMethods.mapColumnIndex(childNode.getOutputSchema());
    updateOutputSchema();
  }

  /**
   * Get the select items of the project operator node
   *
   * @return the select items of the project operator node
   */
  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  /**
   * Set the select items of the project operator node
   *
   * @param selectItems the expected select items of the project operator node
   */
  public void setSelectItems(List<SelectItem> selectItems) {
    this.selectItems = selectItems;
    updateOutputSchema();
  }

  /** updates the output schema based on select items from the select statement. */
  private void updateOutputSchema() {
    ArrayList<Column> outputSchema = new ArrayList<>();
    for (SelectItem item : selectItems) {
      if (item instanceof SelectExpressionItem) {
        // get the column name from the select expression item
        Column column = (Column) ((SelectExpressionItem) item).getExpression();
        int index = columnIndexMap.get(column.getName(true));
        outputSchema.add(childNode.getOutputSchema().get(index));
      } else if (item instanceof AllColumns) {
        outputSchema.addAll(this.outputSchema);
      }
    }
    this.outputSchema = outputSchema;
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
