package operator_node;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperatorNode extends OperatorNode{

  private OperatorNode childNode;
  private Expression whereExpression;

  public SelectOperatorNode(OperatorNode childNode, Expression whereExpression) {
    this.childNode = childNode;
    this.whereExpression = whereExpression;
    this.outputSchema = childNode.getOutputSchema();
  }

  public OperatorNode getChildNode() {
    return childNode;
  }

  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public void setWhereExpression(Expression whereExpression) {
    this.whereExpression = whereExpression;
  }
}
