package operator_node;

import common.OperatorNodeVisitor;

/**
 * EmptyOperatorNode is a class to represent the empty operator in the logical
 * query plan.
 */
public class EmptyOperatorNode extends OperatorNode {

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
