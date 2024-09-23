package operator_node;

import java.util.List;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperatorNode extends OperatorNode{

  private OperatorNode childNode;
  private List<SelectItem> selectItems;

  public ProjectOperatorNode(OperatorNode childNode, List<SelectItem> selectItems) {
    this.childNode = childNode;
    this.selectItems = selectItems;
    this.outputSchema = childNode.getOutputSchema();
  }

  public OperatorNode getChildNode() {
    return childNode;
  }

  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  public void setSelectItems(List<SelectItem> selectItems) {
    this.selectItems = selectItems;
  }
}
