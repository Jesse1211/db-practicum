package operator_node;

import compiler.DBCatalog;
import net.sf.jsqlparser.schema.Table;

/** ScanOperatorNode is a class to represent the scan operator in the logical query plan. */
public class ScanOperatorNode extends OperatorNode {

  private Table table;
  private String indexAttribute = null;
  private int lowerBound;
  private int upperBound;

  /**
   * Set the table as the child to scan operator
   *
   * @param table the table of the scan operator
   */
  public ScanOperatorNode(Table table) {
    this.table = table;
    this.outputSchema = DBCatalog.getInstance().getColumnsWithAlias(table);
  }

  public ScanOperatorNode(Table table, String indexAttribute, int lowerBound, int upperBound) {
    this(table);
    this.indexAttribute = indexAttribute;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public Table getTable() {
    return table;
  }

  public String getIndexAttribute() {
    return indexAttribute;
  }

  public int getLowerBound() {
    return lowerBound;
  }

  public int getUpperBound() {
    return upperBound;
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
