package common;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import operator_node.DuplicateEliminationOperatorNode;
import operator_node.EmptyOperatorNode;
import operator_node.JoinOperatorNode;
import operator_node.OperatorNode;
import operator_node.ProjectOperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;
import operator_node.SortOperatorNode;
import physical_operator.BNLJOperator;
import physical_operator.DuplicateEliminationOperator;
import physical_operator.EmptyOperator;
import physical_operator.ExternalSortOperator;
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
  public PhysicalPlanBuilder() {
  }

  /**
   * @param node
   */
  @Override
  public void visit(DuplicateEliminationOperatorNode node) {
    node.getChildNode().accept(this);
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
        operator = new BNLJOperator(
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

        Pair<Column, Column> columnPair = HelperMethods.getEqualityConditionColumnPair(
                whereExpression);
        if (columnPair == null) {
          System.err.println("SMJ join should provide at least equality condition");
        }

        //get equality condition, extract left and right columns
        SortOperator leftSortOperator = new SortOperator(
                leftOperator.getOutputSchema(),
                leftOperator,
                columnPair.getLeft()
        );
        SortOperator rightSortOperator = new SortOperator(
                rightOperator.getOutputSchema(),
                rightOperator,
                columnPair.getRight()
        );

        operator = new SMJOperator(
                node.getOutputSchema(),
                leftSortOperator,
                rightSortOperator,
                columnPair.getLeft(),
                columnPair.getRight()
        );
        break;
      default:
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

    switch (DBCatalog.getInstance().getSortMethod()) {
      case "In-Memory Sort":
        operator = new SortOperator(node.getOutputSchema(), operator, node.getElementOrders());
        break;
      case "External Sort":
        operator =
            new ExternalSortOperator(
                node.getOutputSchema(),
                operator,
                node.getElementOrders(),
                DBCatalog.getInstance().getSortBufferPageNumber());
        break;
    }
  }

  /**
   * @param node
   */
  @Override
  public void visit(SelectOperatorNode node) {
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
}
