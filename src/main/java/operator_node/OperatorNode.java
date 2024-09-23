package operator_node;

import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;

public abstract class OperatorNode {
  protected ArrayList<Column> outputSchema;

  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  public void setOutputSchema(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }
}
