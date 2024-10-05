package operator_node;

import common.OperatorNodeVisitor;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

public abstract class OperatorNode {
  protected ArrayList<Column> outputSchema;
  protected OperatorNode childNode;
  protected OperatorNode parentNode;

  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  public abstract void accept(OperatorNodeVisitor operatorNodeVisitor);

  public void setOutputSchema(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  public OperatorNode getChildNode() {
    return childNode;
  }

  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  public OperatorNode getParentNode() {
    return parentNode;
  }

  public void setParentNode(OperatorNode parentNode) {
    this.parentNode = parentNode;
  }
}
