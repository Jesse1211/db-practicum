package operator_node;

/** An interface to visit ALL operator nodes for Logical Plan */
public interface OperatorNodeVisitor {
  void visit(DuplicateEliminationOperatorNode node);

  void visit(JoinOperatorNode node);

  void visit(ProjectOperatorNode node);

  void visit(ScanOperatorNode node);

  void visit(SortOperatorNode node);

  void visit(SelectOperatorNode node);

  void visit(EmptyOperatorNode node);
}
