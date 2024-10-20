package operator_node;

import common.OperatorNodeVisitor;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/** SortOperatorNode is a class to represent the sort operator in the logical query plan. */
public class SortOperatorNode extends OperatorNode {

  private List<Column> orders;

  /**
   * Set the node as the child to sort operator
   *
   * @param childNode the child node of the sort operator
   * @param orders the order by columns of the sort operator
   */
  public SortOperatorNode(OperatorNode childNode, List<Column> orders) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.orders = orders;
    this.outputSchema = childNode.getOutputSchema();
  }

  /**
   * Get the order by elements of the sort operator node
   *
   * @return the order by elements of the sort operator node
   */
  public List<Column> getOrders() {
    return orders;
  }

  /**
   * Set the order by elements of the sort operator node
   *
   * @param orders the expected order by elements of the sort operator node
   */
  public void setOrders(List<OrderByElement> orders) {
    this.orders = orders.stream().
            map((orderByElement) -> (Column) orderByElement.getExpression()).toList();
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
