package builder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import common.pair.Pair;
import common.stats.StatsInfo;
import compiler.DBCatalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import operator_node.JoinOperatorNode;
import operator_node.OperatorNode;
import operator_node.ScanOperatorNode;
import operator_node.SelectOperatorNode;

// // Utilize JoinSequenceCreator to get the join order, then combine each element
//     // inside the deque in order.
//     JoinSequenceCreator joinSequenceCreator = new JoinSequenceCreator(node);
//     ArrayDeque<OperatorNode> deque = joinSequenceCreator.getJoinOrder();

public class JoinSequenceCreator {
    List<OperatorNode> nodeList;
    PriorityQueue<Pair<List<String>, Integer>> tableSubsets; // {[table combination], cost}
    ArrayDeque<OperatorNode> joinOrder; // left to right == buttom to top
    Map<Pair<String, String>, Expression> comparisonExpressionMap; // {<table1, table2>, expression}

    public JoinSequenceCreator(JoinOperatorNode joinNode) {
        this.nodeList = new ArrayList<>();
        this.tableSubsets = new PriorityQueue<>((a, b) -> a.getRight() - b.getRight()); // sort cost in ascending order
        this.comparisonExpressionMap = joinNode.getComparisonExpressionMap();
        findAllNodes(joinNode);
        findAllTableSubsets(joinNode.getTableNames(), new ArrayList<>(), tableSubsets, new HashSet<>());
        
        for (Entry entry : comparisonExpressionMap.entrySet()) {
            computeVvalue(entry);
        }
    }

    public ArrayDeque<OperatorNode> getJoinOrder() {
        return joinOrder;
    }

    /**
     * Find all the operator nodes in the tree.
     *
     * @param root
     */
    public void findAllNodes(OperatorNode root) {
        if (!(root instanceof JoinOperatorNode)) {
            return;
        }
        Queue<OperatorNode> queue = new ArrayDeque<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            OperatorNode node = queue.poll();
            nodeList.add(node);

            if (node instanceof JoinOperatorNode) {
                JoinOperatorNode join = (JoinOperatorNode) node;
                for (OperatorNode child : join.getChildNodes()) {
                    queue.offer(child);
                }
            }
        }
    }

    /**
     * Find all the subsets (Combinaion) of the tables.
     *
     * @param index
     * @param cur
     * @param subsets
     * @param visited
     */
    private void findAllTableSubsets(List<String> tableNames, List<String> cur,
            PriorityQueue<Pair<List<String>, Integer>> subsets,
            Set<String> visited) {
        if (cur.size() == tableNames.size() + 1) {
            return;
        }

        for (String tableName : tableNames) {
            if (visited.contains(tableName)) {
                continue;
            }

            cur.add(tableName);
            visited.add(tableName);

            // calculate the cost of the current subset, and add it to the priority queue
            int cost = getCost(cur);
            subsets.offer(new Pair<>(new ArrayList<>(cur), cost));

            // recursively find the next subset
            findAllTableSubsets(tableNames, cur, subsets, visited);
            cur.remove(cur.size() - 1);
            visited.remove(tableName);
        }
    }

    /**
     * Get the cost of the current subset.
     *
     * @param cur: index of the nodes in the subset
     * @return the cost of the subset
     */
    private int getCost(List<String> cur) {
        // cost = sum of tuple sizes of all nodes in the subset
        int cost = 0;
        return 0;
    }

    /**
     * Estimate the cost of joining two nodes.
     *
     * @param vleft
     * @param vright
     * @param leftSize
     * @param rightSize
     */
    private int extimateCostByV(int vleft, int vright, int leftSize, int rightSize, int attributeCount,
            boolean hasEquality) {
        int joinSize = 0;
        if (!hasEquality) {
            // Does not have equality comparison, consider as cross product
            joinSize = leftSize * rightSize;
        } else {
            // Disregard the unEqualities
            if (attributeCount == 1) {
                // Join ONE 'same attribute'
                joinSize = (leftSize * rightSize) / Math.max(vleft, vright);
            } else {
                // Join N 'different attributes'
                joinSize = (leftSize * rightSize) / (Math.max(vleft, vright) * Math.min(vleft,
                        vright));
            }
        }
        return joinSize;
    }

    /**
     * Get the number of distinct values that attribute A takes in table R
     *
     * @param R table
     * @param A attribute
     * @return V-value
     */
    private int vValue(Table R, Column A) {
        StatsInfo statsInfo = DBCatalog.getInstance().getStatsInfo(R.getName());
        int max = statsInfo.columnStats.get(A.getColumnName()).getRight();
        int min = statsInfo.columnStats.get(A.getColumnName()).getLeft();
        return max - min + 1;
    }

    /**
     * Get the number of distinct values that attribute A takes in table R
     *
     * @param R table
     * @param A attribute
     * @return V-value
     */
    private int computeVvalue(Entry entry) {
        Pair<String, String> key = (Pair<String, String>) entry.getKey();
        String table1 = key.getLeft();
        String table2 = key.getRight();
        String expression = ((Expression) entry.getValue()).toString();

        String a = "1";
        if (expression.contains(" = ")) {
            // Selection on base table
        }

        // Table R = ((ScanOperatorNode) operatorNode).getTable();
        // if (operatorNode instanceof SelectOperatorNode) {
        //     // Base table
        //     Column A = ((SelectOperatorNode) operatorNode).getOutputSchema().getFirst();
        //     return vValue(R, A);
        // // } else if (isSelectionOnBaseTable) {
        // //     // Derived table
        // //     return 0;
        // } else {
        //     // Join
        //     return 0;
        // }
        return 0;
    }
}
