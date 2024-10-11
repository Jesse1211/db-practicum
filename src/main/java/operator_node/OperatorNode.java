package operator_node;

import common.OperatorNodeVisitor;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

/**
 * OperatorNode is a class to represent the operator nodes in the logical query
 * plan.
 */
public abstract class OperatorNode {
  protected ArrayList<Column> outputSchema;
  protected OperatorNode childNode;
  protected OperatorNode parentNode;

  /**
   * @return the outputSchema of the operator node
   */
  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /**
   * Visit the current class by the operatorNodeVisitor, next step should accept
   * the child's node in a recursive manner
   * 
   * @param operatorNodeVisitor
   */
  public abstract void accept(OperatorNodeVisitor operatorNodeVisitor);

  /**
   * Set the output schema of the operator node
   * 
   * @param outputSchema the expected output schema of the operator node
   */
  public void setOutputSchema(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Get the child node of the operator node
   * 
   * @return the child node of the operator node
   */
  public OperatorNode getChildNode() {
    return childNode;
  }

  /**
   * Set the child node of the operator node
   * 
   * @param childNode the expected child node of the operator node
   */
  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  /**
   * Get the parent node of the operator node
   * 
   * @return the parent node of the operator node
   */
  public OperatorNode getParentNode() {
    return parentNode;
  }

  /**
   * Set the parent node of the operator node
   * 
   * @param parentNode the expected parent node of the operator node
   */
  public void setParentNode(OperatorNode parentNode) {
    this.parentNode = parentNode;
  }
}
