package operator_node;

import java.util.ArrayList;

/** JoinOperatorNode is a class to represent the join operator in the logical query plan. */
public class JoinOperatorNode extends OperatorNode {
  private OperatorNode leftChildNode;
  private OperatorNode rightChildNode;

  /**
   * Set the left and right child nodes to the join operator
   *
   * @param leftChildNode the left child node of the join operator
   * @param rightChildNode the right child node of the join operator
   */
  public JoinOperatorNode(OperatorNode leftChildNode, OperatorNode rightChildNode) {

    this.leftChildNode = leftChildNode;
    this.rightChildNode = rightChildNode;
    this.leftChildNode.setParentNode(this);
    this.rightChildNode.setParentNode(this);

    this.outputSchema = new ArrayList<>(leftChildNode.getOutputSchema());
    this.outputSchema.addAll(rightChildNode.getOutputSchema());
  }

  public OperatorNode getLeftChildNode() {
    return leftChildNode;
  }

  public OperatorNode getRightChildNode() {
    return rightChildNode;
  }

  public void setLeftChildNode(OperatorNode leftChildNode) {
    this.leftChildNode = leftChildNode;
  }

  public void setRightChildNode(OperatorNode rightChildNode) {
    this.rightChildNode = rightChildNode;
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }

  @Override
  public OperatorNode getChildNode() {
    System.out.println(
        "JoinOperator should not have a single child, used getLeftChildNode and getRightChildNode instead.");
    return null;
  }
}
