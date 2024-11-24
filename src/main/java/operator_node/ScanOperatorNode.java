package operator_node;

import compiler.DBCatalog;
import net.sf.jsqlparser.schema.Table;

/** ScanOperatorNode is a class to represent the scan operator in the logical query plan. */
public class ScanOperatorNode extends OperatorNode {

  private Table table;

  /**
   * Set the table as the child to scan operator
   *
   * @param table the table of the scan operator
   */
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
