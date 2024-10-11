package common;

import operator_node.DuplicateEliminationOperatorNode;
import operator_node.EmptyOperatorNode;
import operator_node.JoinOperatorNode;
import operator_node.ProjectOperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;
import operator_node.SortOperatorNode;

/**
 * An interface to visit ALL operator nodes for Logical Plan
 */
public interface OperatorNodeVisitor {
  void visit(DuplicateEliminationOperatorNode node);

  void visit(JoinOperatorNode node);

  void visit(ProjectOperatorNode node);

  void visit(ScanOperatorNode node);

  void visit(SortOperatorNode node);

  void visit(SelectOperatorNode node);

  void visit(EmptyOperatorNode node);
}
