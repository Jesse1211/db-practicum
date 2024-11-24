package builder;

import static java.lang.System.exit;

import common.HelperMethods;
import common.pair.Pair;
import compiler.DBCatalog;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator_node.DuplicateEliminationOperatorNode;
import operator_node.EmptyOperatorNode;
import operator_node.JoinOperatorNode;
import operator_node.OperatorNode;
import operator_node.OperatorNodeVisitor;
import operator_node.ProjectOperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;
import operator_node.SortOperatorNode;
import physical_operator.BNLJOperator;
import physical_operator.DuplicateEliminationOperator;
import physical_operator.EmptyOperator;
import physical_operator.ExternalSortOperator;
import physical_operator.IndexScanOperator;
import physical_operator.JoinOperator;
import physical_operator.Operator;
import physical_operator.ProjectOperator;
import physical_operator.SMJOperator;
import physical_operator.ScanOperator;
import physical_operator.SelectOperator;
import physical_operator.SortOperator;

/**
 * PhysicalPlanBuilder is a class to build the physical query plan based on relational algebra query
 * plan.
 */
public class PhysicalPlanBuilder implements OperatorNodeVisitor {

  private Operator operator;

  /**
   * PhysicalPlanBuilder is a class to build the physical query plan based on relational algebra
   * query plan.
   */
  public PhysicalPlanBuilder() {}

  /**
   * @param node
   */
  @Override
  public void visit(DuplicateEliminationOperatorNode node) {
    node.getChildNode().accept(this);

    // Use sort to process distinct, if the child is not SortOperator, we create a sort node.
    if (!(node.getChildNode() instanceof SortOperatorNode)) {
      operator = getSortOperator(new SortOperatorNode(node, new ArrayList<>()), operator);
    }
    operator = new DuplicateEliminationOperator(node.getOutputSchema(), operator);
  }

  /**
   * @param node
   */
  @Override
  public void visit(JoinOperatorNode node) {
    node.getLeftChildNode().accept(this);
    Operator leftOperator = operator;
    node.getRightChildNode().accept(this);
    Operator rightOperator = operator;

    // read from config.properties to select the join method
    switch (DBCatalog.getInstance().getJoinMethod()) {
      case "TNLJ":
        operator = new JoinOperator(node.getOutputSchema(), leftOperator, rightOperator);
        break;
      case "BNLJ":
        operator =
            new BNLJOperator(
                node.getOutputSchema(),
                leftOperator,
                rightOperator,
                DBCatalog.getInstance().getJoinBufferPageNumber());
        break;
      case "SMJ":
        OperatorNode parent = node.getParentNode();
        if (parent == null || !(parent instanceof SelectOperatorNode)) {
          System.err.println("SMJ join should provide at least equality condition");
        }
        Expression whereExpression = ((SelectOperatorNode) parent).getWhereExpression();

        Pair<Column, Column> columnPair =
            HelperMethods.getEqualityConditionColumnPair(
                whereExpression, leftOperator, rightOperator);
        if (columnPair == null) {
          System.err.println("SMJ join should provide at least equality condition");
          exit(-1);
        }

        // get equality condition, extract left and right columns
        Operator leftSortOperator =
            getSortOperator(
                new SortOperatorNode(
                    node.getLeftChildNode(), Collections.singletonList(columnPair.getLeft())),
                leftOperator);
        Operator rightSortOperator =
            getSortOperator(
                new SortOperatorNode(
                    node.getRightChildNode(), Collections.singletonList(columnPair.getRight())),
                rightOperator);

        operator =
            new SMJOperator(
                node.getOutputSchema(),
                leftSortOperator,
                rightSortOperator,
                columnPair.getLeft(),
                columnPair.getRight());
        break;
    }
  }

  /**
   * @param node
   */
  @Override
  public void visit(ProjectOperatorNode node) {
    node.getChildNode().accept(this);
    operator = new ProjectOperator(node.getOutputSchema(), operator);
  }

  /**
   * @param node
   */
  @Override
  public void visit(ScanOperatorNode node) {
    operator = new ScanOperator(node.getTable());
  }

  /**
   * @param node
   */
  @Override
  public void visit(SortOperatorNode node) {
    node.getChildNode().accept(this);
    operator = getSortOperator(node, operator);
  }

  /**
   * @param node
   */
  @Override
  public void visit(SelectOperatorNode node) {
    // check if we should apply index
    if (DBCatalog.getInstance().getUseIndex() && node.getChildNode() instanceof ScanOperatorNode) {
      Table table = ((ScanOperatorNode) node.getChildNode()).getTable();
      List<ComparisonOperator> flattened =
          HelperMethods.flattenExpression(node.getWhereExpression());
      List<ComparisonOperator> indexedComparisons =
          HelperMethods.getIndexedComparisons(flattened, table);

      // if comparison contains index comparisons, set the operator to indexScanOperator, otherwise,
      // use original ScanOperator
      if (indexedComparisons.size() > 0) {
        Pair<Integer, Integer> keyPair = HelperMethods.getLowKeyHighKey(indexedComparisons);
        operator =
            new IndexScanOperator(
                node.getOutputSchema(), keyPair.getLeft(), keyPair.getRight(), table);
      } else {
        node.getChildNode().accept(this);
      }

      // if there are non index comparisons, add it to the selectOperator.
      Expression nonIndexedComparison =
          HelperMethods.getNonIndexedComparisons(flattened, indexedComparisons);
      if (nonIndexedComparison != null) {
        operator = new SelectOperator(node.getOutputSchema(), operator, nonIndexedComparison);
      }
      return;
    }
    node.getChildNode().accept(this);
    operator = new SelectOperator(node.getOutputSchema(), operator, node.getWhereExpression());
  }

  /**
   * @param node
   */
  @Override
  public void visit(EmptyOperatorNode node) {
    operator = new EmptyOperator();
  }

  /**
   * @return the operator
   */
  public Operator getResult() {
    return operator;
  }

  private Operator getSortOperator(SortOperatorNode node, Operator childOpeartor) {
    switch (DBCatalog.getInstance().getSortMethod()) {
      case "In-Memory Sort":
        return new SortOperator(node.getOutputSchema(), childOpeartor, node.getOrders());
      case "External Sort":
        return new ExternalSortOperator(
            node.getOutputSchema(),
            childOpeartor,
            node.getOrders(),
            DBCatalog.getInstance().getSortBufferPageNumber());
    }
    return new SortOperator(node.getOutputSchema(), childOpeartor, node.getOrders());
  }
}
