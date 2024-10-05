package operator_node;

import common.OperatorNodeVisitor;
import java.util.ArrayList;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import net.sf.jsqlparser.schema.Column;

public class JoinOperatorNode extends OperatorNode {
  private OperatorNode leftChildNode;
  private OperatorNode rightChildNode;

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

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }

  @Override
  public OperatorNode getChildNode(){
    System.out.println("JoinOperator should not have a single child, used getLeftChildNode and getRightChildNode instead.");
    return null;
  }
}
