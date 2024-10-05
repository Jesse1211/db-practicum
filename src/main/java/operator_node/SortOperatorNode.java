package operator_node;

import common.OperatorNodeVisitor;
import java.util.List;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperatorNode extends OperatorNode{

  private List<OrderByElement> elementOrders;

  public SortOperatorNode(OperatorNode childNode, List<OrderByElement> elementOrders) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.elementOrders = elementOrders;
    this.outputSchema = childNode.getOutputSchema();
  }

  public List<OrderByElement> getElementOrders() {
    return elementOrders;
  }

  public void setElementOrders(List<OrderByElement> elementOrders) {
    this.elementOrders = elementOrders;
  }

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
