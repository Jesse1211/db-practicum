package operator_node;

import common.DBCatalog;
import net.sf.jsqlparser.schema.Table;

public class ScanOperatorNode extends OperatorNode{

  private Table table;
  private OperatorNode childNode;

  public ScanOperatorNode(OperatorNode childNode, Table table) {
    this.childNode = childNode;
    this.table = table;
    this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
  }

  public OperatorNode getChildNode() {
    return childNode;
  }

  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }
}
