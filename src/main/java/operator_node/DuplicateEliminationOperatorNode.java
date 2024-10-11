package operator_node;

import common.OperatorNodeVisitor;

/**
 * DuplicateEliminationOperatorNode is a class to represent the duplicate elimination operator in
 * the logical query plan.
 */
public class DuplicateEliminationOperatorNode extends OperatorNode {

  /**
   * Set the node as the child to duplicate elimination operator
   *
   * @param childNode the child node of the duplicate elimination operator
   */
  public DuplicateEliminationOperatorNode(OperatorNode childNode) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.outputSchema = childNode.getOutputSchema();
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
