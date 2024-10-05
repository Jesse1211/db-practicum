package operator_node;

import common.OperatorNodeVisitor;

public class EmptyOperatorNode extends OperatorNode{

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
