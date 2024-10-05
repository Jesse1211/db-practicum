package operator_node;

import common.DBCatalog;
import common.OperatorNodeVisitor;
import net.sf.jsqlparser.schema.Table;

public class ScanOperatorNode extends OperatorNode {

  private Table table;

  public ScanOperatorNode(Table table) {
    this.table = table;
    this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  /**
   * @param operatorNodeVisitor
   */
  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }

  @Override
  public OperatorNode getChildNode() {
    System.out.println("JoinOperator should not have a child.");
    return null;
  }
}
