package operator_node;

import common.OperatorNodeVisitor;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperatorNode extends OperatorNode{

  private Expression whereExpression;

  public SelectOperatorNode(OperatorNode childNode, Expression whereExpression) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.whereExpression = whereExpression;
    this.outputSchema = childNode.getOutputSchema();
  }

  public Expression getWhereExpression() {
    return whereExpression;
  }

  public void setWhereExpression(Expression whereExpression) {
    this.whereExpression = whereExpression;
  }

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
