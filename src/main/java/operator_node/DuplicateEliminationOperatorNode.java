package operator_node;

import common.OperatorNodeVisitor;

public class DuplicateEliminationOperatorNode extends OperatorNode {

  public DuplicateEliminationOperatorNode(OperatorNode childNode) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.outputSchema = childNode.getOutputSchema();
  }

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
