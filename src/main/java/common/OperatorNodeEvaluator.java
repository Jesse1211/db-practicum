package common;

import operator_node.DuplicateEliminationOperatorNode;
import operator_node.EmptyOperatorNode;
import operator_node.JoinOperatorNode;
import operator_node.ProjectOperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;
import operator_node.SortOperatorNode;
import physical_operator.DuplicateEliminationOperator;
import physical_operator.EmptyOperator;
import physical_operator.JoinOperator;
import physical_operator.Operator;
import physical_operator.ProjectOperator;
import physical_operator.ScanOperator;
import physical_operator.SelectOperator;
import physical_operator.SortOperator;

public class OperatorNodeEvaluator implements OperatorNodeVisitor {

  private Operator operator;

  public OperatorNodeEvaluator() {}

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
    operator = new JoinOperator(node.getOutputSchema(), leftOperator, rightOperator);
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
    operator = new SortOperator(node.getOutputSchema(), operator, node.getElementOrders());
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

  public Operator getResult() {
    return operator;
  }
}
