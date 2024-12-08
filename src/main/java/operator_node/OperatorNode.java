package operator_node;

import common.HelperMethods;
import common.UnionFind;
import common.UnionFindElement;
import java.util.ArrayList;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/** OperatorNode is a class to represent the operator nodes in the logical query plan. */
public abstract class OperatorNode {
  protected ArrayList<Column> outputSchema;
  protected OperatorNode childNode;
  protected OperatorNode parentNode;

  /**
   * @return the outputSchema of the operator node
   */
  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  /**
   * Visit the current class by the operatorNodeVisitor, next step should accept the child's node in
   * a recursive manner
   *
   * @param operatorNodeVisitor
   */
  public abstract void accept(OperatorNodeVisitor operatorNodeVisitor);

  /**
   * Set the output schema of the operator node
   *
   * @param outputSchema the expected output schema of the operator node
   */
  public void setOutputSchema(ArrayList<Column> outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Get the child node of the operator node
   *
   * @return the child node of the operator node
   */
  public OperatorNode getChildNode() {
    return childNode;
  }

  /**
   * Set the child node of the operator node
   *
   * @param childNode the expected child node of the operator node
   */
  public void setChildNode(OperatorNode childNode) {
    this.childNode = childNode;
  }

  /**
   * Get the parent node of the operator node
   *
   * @return the parent node of the operator node
   */
  public OperatorNode getParentNode() {
    return parentNode;
  }

  /**
   * Set the parent node of the operator node
   *
   * @param parentNode the expected parent node of the operator node
   */
  public void setParentNode(OperatorNode parentNode) {
    this.parentNode = parentNode;
  }

  /** Print the logical query plan in term of a tree structure */
  public StringBuilder print() {
    StringBuilder tree = new StringBuilder();
    dfs(tree, this, 0);
    return tree;
  }

  /**
   * Depth first search to print the logical query plan in term of a tree structure
   *
   * @param tree the tree structure to store the logical query plan
   * @param cur the current operator node
   * @param level the current level of the tree
   */
  private void dfs(StringBuilder tree, OperatorNode cur, int level) {
    for (int i = 0; i < level; i++) {
      tree.append("-");
    }

    if (cur instanceof EmptyOperatorNode) {
      tree.append("Leaf[null]\n");
    } else if (cur instanceof DuplicateEliminationOperatorNode) {
      tree.append("DupElim\n");
      if (cur.getChildNode() != null) {
        dfs(tree, cur.getChildNode(), level + 1);
      }
    } else if (cur instanceof SortOperatorNode) {
      tree.append("Sort[")
          .append(HelperMethods.convertColumnList(cur.getOutputSchema()))
          .append("]\n");
      dfs(tree, cur.getChildNode(), level + 1);
    } else if (cur instanceof ProjectOperatorNode) {
      tree.append("Project[")
          .append(HelperMethods.convertColumnList(cur.getOutputSchema()))
          .append("]\n");
      dfs(tree, cur.getChildNode(), level + 1);
    } else if (cur instanceof SelectOperatorNode) {
      tree.append("Select");
      Expression expression = ((SelectOperatorNode) cur).getWhereExpression();
      tree.append("[" + expression.toString() + "]\n");
      dfs(tree, cur.getChildNode(), level + 1);
    } else if (cur instanceof JoinOperatorNode) {
      tree.append("Join");

      // print residual joins
      Set<ComparisonOperator> comparisons = ((JoinOperatorNode) cur).getComparisons();
      boolean hasResidualJoin = false;
      for (ComparisonOperator comparison : comparisons) {
        hasResidualJoin = true;
        if (comparison instanceof NotEqualsTo) {
          tree.append("[" + comparison.toString() + "]\n");
        }
      }

      if (!hasResidualJoin) {
        tree.append("[null]\n");
      }

      // print union find
      UnionFind unionFind = UnionFind.getInstance(false);

      for (UnionFindElement unionFindElement : unionFind.getElements()) {
        tree.append("[");
        // Print the attributes of the UnionFindElement
        tree.append("[");

        // use index to define if the attribute is the last one
        int index = 1;
        for (String name : unionFindElement.attributes.keySet()) {
          tree.append(name);
          if (index++ != unionFindElement.attributes.size()) {
            tree.append(", ");
          }
        }
        tree.append("], ");

        // Print the equality conditions of the UnionFindElement
        String lower =
            unionFindElement.lowerBound == Integer.MIN_VALUE
                ? "null"
                : unionFindElement.lowerBound + "";
        String upper =
            unionFindElement.upperBound == Integer.MAX_VALUE
                ? "null"
                : unionFindElement.upperBound + "";
        tree.append("equals null, min " + lower + ", max " + upper);

        tree.append("]\n");
      }

      for (OperatorNode child : ((JoinOperatorNode) cur).getChildNodes()) {
        dfs(tree, child, level + 1);
      }
    } else if (cur instanceof ScanOperatorNode) {
      tree.append("Leaf[" + cur.getOutputSchema().get(0).getTable().getName() + "]\n");
    }
  }
}
