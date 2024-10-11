package operator_node;

import common.OperatorNodeVisitor;
import net.sf.jsqlparser.expression.Expression;

/** SelectOperatorNode is a class to represent the select operator in the logical query plan. */
public class SelectOperatorNode extends OperatorNode {

  private Expression whereExpression;

  /**
   * Set the node as the child to select operator
   *
   * @param childNode the child node of the select operator
   * @param whereExpression the where expression of the select operator
   */
  public SelectOperatorNode(OperatorNode childNode, Expression whereExpression) {
    this.childNode = childNode;
    this.childNode.setParentNode(this);
    this.whereExpression = whereExpression;
    this.outputSchema = childNode.getOutputSchema();
  }

  /**
   * Get the where expression of the select operator node
   *
   * @return the where expression of the select operator node
   */
  public Expression getWhereExpression() {
    return whereExpression;
  }

  /**
   * Set the where expression of the select operator node
   *
   * @param whereExpression the expected where expression of the select operator node
   */
  public void setWhereExpression(Expression whereExpression) {
    this.whereExpression = whereExpression;
  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }
}
