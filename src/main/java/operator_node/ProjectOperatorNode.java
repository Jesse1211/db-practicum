package operator_node;

import common.HelperMethods;
import common.OperatorNodeVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperatorNode extends OperatorNode {

  private final Map<String, Integer> columnIndexMap;
  private List<SelectItem> selectItems;

  public ProjectOperatorNode(OperatorNode childNode, List<SelectItem> selectItems) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.selectItems = selectItems;
    this.columnIndexMap = HelperMethods.mapColumnIndex(childNode.getOutputSchema());
    updateOutputSchema();
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  public void setSelectItems(List<SelectItem> selectItems) {
    this.selectItems = selectItems;
    updateOutputSchema();
  }

  /** updates the output schema based on select items from the select statement. */
  private void updateOutputSchema() {
    ArrayList<Column> outputSchema = new ArrayList<>();
    for (SelectItem item : selectItems) {
      if (item instanceof SelectExpressionItem) {
        Column column = (Column) ((SelectExpressionItem) item).getExpression();
        int index = columnIndexMap.get(column.getName(true));
        outputSchema.add(childNode.getOutputSchema().get(index));
      } else if (item instanceof AllColumns) {
        outputSchema.addAll(this.outputSchema);
      }
    }
    this.outputSchema = outputSchema;
  }

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
