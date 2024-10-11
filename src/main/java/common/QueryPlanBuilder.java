package common;

import net.sf.jsqlparser.statement.Statement;
import operator_node.OperatorNode;
import physical_operator.Operator;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {

  /**
   * Top level method to translate statement to query plan
   *
   * <p>1. Build the logical plan as a Tree of OperatorNodes
   *
   * <p>2. Translate the Tree to a physical plan by utilizing the Visitor pattern
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  public Operator buildPlan(Statement stmt) {
    OperatorNode root = LogicalPlanBuilder.buildPlan(stmt);

    // Translate to physical plan
    PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder();
    root.accept(physicalPlanBuilder);
    return physicalPlanBuilder.getResult();
  }
}
