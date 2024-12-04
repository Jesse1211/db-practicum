package builder;

import common.pair.Pair;
import compiler.DBCatalog;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
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
import physical_operator.Operator;
import physical_operator.ProjectOperator;
import physical_operator.SMJOperator;
import physical_operator.ScanOperator;
import physical_operator.SelectOperator;

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

    // Use sort to process distinct, if the child is not SortOperator, we create a
    // sort node.
    if (!(node.getChildNode() instanceof SortOperatorNode)) {
      operator =
          new ExternalSortOperator(
              node.getOutputSchema(),
              operator,
              new ArrayList<>(),
              DBCatalog.getInstance().getSortBufferPageNumber());
    }
    operator = new DuplicateEliminationOperator(node.getOutputSchema(), operator);
  }

  /**
   * @param node
   */
  @Override
  public void visit(JoinOperatorNode node) {

    // Utilize JoinSequenceCreator to get the join order, then combine each element
    // inside the deque in order.
    JoinSequenceCreator joinSequenceCreator = new JoinSequenceCreator(node);
    ArrayDeque<OperatorNode> deque = joinSequenceCreator.getJoinOrder();
    Set<Pair<String, String>> equalityJoinMap = node.getEqualityJoinMap();

    // ArrayDeque<OperatorNode> deque = new ArrayDeque<>(node.getChildNodes());

    List<String> tableNames = node.getTableAliasNames();

    // start from index 0, indicating current table to join left. Every time we join
    // a right table,
    // we increment by 1.
    int currentIndex = 0;

    assert deque.size() >= 2;
    deque.poll().accept(this);
    Operator left = operator;
    currentIndex++;

    while (!deque.isEmpty()) {
      deque.poll().accept(this);
      Operator right = operator;

      ArrayList<Column> outputSchema = new ArrayList<>();

      Boolean reverse = null;
      for (Column c : node.getOutputSchema()) {
        for (Column cLeft : left.getOutputSchema()) {
          if (cLeft.getName(true).equals(c.getName(true))) {
            outputSchema.add(cLeft);
            if (reverse == null) reverse = false;
          }
        }
        for (Column cRight : right.getOutputSchema()) {
          if (cRight.getName(true).equals(c.getName(true))) {
            outputSchema.add(cRight);
            if (reverse == null) reverse = true;
          }
        }
      }

      String join = "BNLJ";
      Pair<Column, Column> columnPair = null;
      for (Column cLeft : left.getOutputSchema()) {
        for (Column cRight : right.getOutputSchema()) {
          String leftName = cLeft.getName(true);
          String rightName = cRight.getName(true);
          if (equalityJoinMap.contains(new Pair<>(leftName, rightName))) {
            join = "SMJ";
            columnPair = new Pair<>(cLeft, cRight);
          }
        }
      }

      // choose which to join here :)

      if (join.equals("BNLJ")) {
        left =
            new BNLJOperator(
                outputSchema,
                left,
                right,
                DBCatalog.getInstance().getJoinBufferPageNumber(),
                reverse);
      } else {
        // get equality condition, extract left and right columns
        Operator leftSortOperator =
            new ExternalSortOperator(
                left.getOutputSchema(),
                left,
                Collections.singletonList(columnPair.getLeft()),
                DBCatalog.getInstance().getSortBufferPageNumber());
        Operator rightSortOperator =
            new ExternalSortOperator(
                right.getOutputSchema(),
                right,
                Collections.singletonList(columnPair.getRight()),
                DBCatalog.getInstance().getSortBufferPageNumber());

        left =
            new SMJOperator(
                outputSchema,
                leftSortOperator,
                rightSortOperator,
                columnPair.getLeft(),
                columnPair.getRight(),
                reverse);
      }

      // Find all residual join comparisons related to current table
      Expression expression = null;
      for (int prev = 0; prev < currentIndex; prev++) {
        Expression new_expression =
            node.getComparisonExpressionMap()
                .getOrDefault(new Pair<>(tableNames.get(currentIndex), tableNames.get(prev)), null);
        if (new_expression == null) continue;

        if (expression == null) {
          expression = new_expression;
        } else {
          expression = new AndExpression(expression, new_expression);
        }
      }

      if (expression != null) {
        left = new SelectOperator(outputSchema, left, expression);
      }
      currentIndex++;
    }

    operator = left;
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
    if (node.getIndexAttribute() == null) {
      operator = new ScanOperator(node.getTable());
    } else {
      operator =
          new IndexScanOperator(
              node.getLowerBound(),
              node.getUpperBound(),
              node.getTable(),
              node.getIndexAttribute());
    }
  }

  /**
   * @param node
   */
  @Override
  public void visit(SortOperatorNode node) {
    node.getChildNode().accept(this);
    operator =
        new ExternalSortOperator(
            node.getOutputSchema(),
            operator,
            node.getOrders(),
            DBCatalog.getInstance().getSortBufferPageNumber());
  }

  /**
   * @param node
   */
  @Override
  public void visit(SelectOperatorNode node) {
    // // check if we should apply index
    // if (DBCatalog.getInstance().getUseIndex() && node.getChildNode() instanceof
    // ScanOperatorNode) {
    // Table table = ((ScanOperatorNode) node.getChildNode()).getTable();
    // List<ComparisonOperator> flattened =
    // HelperMethods.flattenExpression(node.getWhereExpression());
    // List<ComparisonOperator> indexedComparisons =
    // HelperMethods.getIndexedComparisons(flattened, table);
    //
    // // if comparison contains index comparisons, set the operator to
    // indexScanOperator, otherwise,
    // // use original ScanOperator
    // if (indexedComparisons.size() > 0) {
    // Pair<Integer, Integer> keyPair =
    // HelperMethods.getLowKeyHighKey(indexedComparisons);
    // operator = new IndexScanOperator(node.getOutputSchema(), keyPair.getLeft(),
    // keyPair.getRight(), table);
    // } else {
    // node.getChildNode().accept(this);
    // }
    //
    // // if there are non index comparisons, add it to the selectOperator.
    // Expression nonIndexedComparison =
    // HelperMethods.getNonIndexedComparisons(flattened, indexedComparisons);
    // if (nonIndexedComparison != null) {
    // operator = new SelectOperator(node.getOutputSchema(), operator,
    // nonIndexedComparison);
    // }
    // return;
    // }
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

  //  private Operator getSortOperator(SortOperatorNode node, Operator childOpeartor) {
  //    switch (DBCatalog.getInstance().getSortMethod()) {
  //      case "In-Memory Sort":
  //        return new SortOperator(node.getOutputSchema(), childOpeartor, node.getOrders());
  //      case "External Sort":
  //        return new ExternalSortOperator(
  //            node.getOutputSchema(),
  //            childOpeartor,
  //            node.getOrders(),
  //            DBCatalog.getInstance().getSortBufferPageNumber());
  //    }
  //    return new SortOperator(node.getOutputSchema(), childOpeartor, node.getOrders());
  //  }
}
