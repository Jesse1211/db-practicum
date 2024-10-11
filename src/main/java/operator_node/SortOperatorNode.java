package operator_node;

import common.OperatorNodeVisitor;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

/** SortOperatorNode is a class to represent the sort operator in the logical query plan. */
public class SortOperatorNode extends OperatorNode {

  private List<OrderByElement> elementOrders;

  /**
   * Set the node as the child to sort operator
   *
   * @param childNode the child node of the sort operator
   * @param elementOrders the order by elements of the sort operator
   */
  public SortOperatorNode(OperatorNode childNode, List<OrderByElement> elementOrders) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.elementOrders = elementOrders;
    this.outputSchema = childNode.getOutputSchema();
  }

  /**
   * Get the order by elements of the sort operator node
   *
   * @return the order by elements of the sort operator node
   */
  public List<OrderByElement> getElementOrders() {
    return elementOrders;
  }

  /**
   * Set the order by elements of the sort operator node
   *
   * @param elementOrders the expected order by elements of the sort operator node
   */
  public void setElementOrders(List<OrderByElement> elementOrders) {
    this.elementOrders = elementOrders;
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
