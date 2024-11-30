package operator_node;

import common.HelperMethods;
import common.pair.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Table;

/** JoinOperatorNode is a class to represent the join operator in the logical query plan. */
public class JoinOperatorNode extends OperatorNode {
  private List<OperatorNode> childNodes;
  private List<String> tableNames;
  private Set<ComparisonOperator> comparisons;
  private Map<Pair<String, String>, Expression> comparisonExpressionMap;

  /**
   * Set the left and right child nodes to the join operator
   *
   * @param childNodes the child nodes of the join operator
   */
  public JoinOperatorNode(List<String> tableNames, List<OperatorNode> childNodes, Set<ComparisonOperator> comparisons) {
    this.childNodes = childNodes;
    this.comparisons = comparisons;
    this.tableNames = tableNames;
    this.outputSchema = new ArrayList<>();
    for (OperatorNode child: childNodes){
      this.outputSchema.addAll(child.getOutputSchema());
    }
    createComparisonExpressionMap();
  }

  private void createComparisonExpressionMap(){
    this.comparisonExpressionMap = new HashMap<>();
    for (ComparisonOperator comparison: comparisons){
      Pair<String, String> tableNamePair = HelperMethods.getComparisonTableNames(comparison);
      Expression expression = comparisonExpressionMap.getOrDefault(tableNamePair, null);
      if (expression == null) {
        comparisonExpressionMap.put(tableNamePair, comparison);
      } else {
        comparisonExpressionMap.put(tableNamePair, new AndExpression(expression, comparison));
      }
    }
  }

//  public OperatorNode getLeftChildNode() {
//    return leftChildNode;
//  }

//  public OperatorNode getRightChildNode() {
//    return rightChildNode;
//  }

//  public void setLeftChildNode(OperatorNode leftChildNode) {
//    this.leftChildNode = leftChildNode;
//  }
//
//  public void setRightChildNode(OperatorNode rightChildNode) {
//    this.rightChildNode = rightChildNode;
//  }

  @Override
  public void accept(OperatorNodeVisitor operatorNodeVisitor) {
    operatorNodeVisitor.visit(this);
  }

  public List<OperatorNode> getChildNodes(){
    return this.childNodes;
  }

  public Set<ComparisonOperator> getComparisons() {
    return comparisons;
  }

  public Map<Pair<String, String>, Expression> getComparisonExpressionMap() {
    return comparisonExpressionMap;
  }

  public List<String> getTableNames(){
    return this.tableNames;
  }

  @Override
  public OperatorNode getChildNode() {
    System.out.println(
        "JoinOperator should not have a single child, used getLeftChildNode and getRightChildNode instead.");
    return null;
  }

}
