# Phase 4

## Top Level Class
The top level class is Compiler, which contains a main method that takes a single parameter
of the interpreter config file.

## Selection Pushing
The selection pushing is implemented in `src/main/common/ComparisonEvaluator`, which utilizes
`UnionFind` class to build a set of `UnionFindElement`, each `UnionFindElement` is a cluster of columns
that share the same lower bound and upper bound value.

## Choice of Implementation For Each Logical Selection Operator
The choice of logical selection operator in our project is the same as deciding whether to use
a indexScan or a regular scan. Our project follows the instruction is calculating the estimate cost using 
the formula provided.  

**Cost of using index:** \
If `p` is the number of pages in the relation, `t` the number of tuples, `r` the reduction factor and `l` the
number of leaves in the index, the cost for a clustered index is `3 + p ∗ r` while for an unclustered index it is
`3 + l ∗ r + t ∗ r`. The cost 

**Cost of not using index(regular scan):** \
Page number of stored data. Calculating of the number of page will be `Ceil(numAttribute * numTuple * 4 / pageSize)`

These code of choosing index can be found in `/src/main/builder/LogicalPlanBuilder`'s `chooseScanIndex` method.

## Choice of Join Order
The choice of join order uses a dynamic programming approach described in the instruction. 


The code can be found in `/src/main/builder/JoinSequenceCreator`

## Choice of Implementation of Each Join Operator
The choice of join operator is also following the example given in instruction:
`"SMJ runs much faster than BNLJ, so I implement all joins as SMJ where possible. However, SMJ does not
apply to joins that have other-than-equality comparisons or to pure cross-products, so those are
implemented using BNLJ"`

The code can be found in `/src/main/builder/PhysicalPlanBuilder` in `public void visit(JoinOperatorNode node)`

# Phase 3

For previous phases, check `README_Phase2.md`.

This phase is focusing on index related functionalities.

## B+ Tree

Each node contains 4096 bytes flat, no need to worry about space exceeding.

### IndexBuilder

- Triggered by `build()` according to configuration.
- If clustered index, it will sort the data and replace original relation file.

### IndexDeserializer

- Triggered by `IndexScanOperator` according to configuration.
- Traverse from 'head' to leaf by default to prepare for tuple fetch.
- Two functions to handle each leaf / index accordinly.
- If clustered index, it will reset tuple reader at first time, then scan sequentially.
- If unclustered index, every fetch requires a set and get for the tuple reader

### Structure

#### Header node

- the address of the root, stored at offset 0 on the header page
- the number of leaves in the tree, at offset 4
- the order of the tree, at offset 8.

#### Index Node

- the integer 1 as a flag to indicate this is an index node (rather than a leaf node)
- the number of keys in the node
- the actual keys in the node, in order
- the addresses of all the children of the node, in order

#### Leaf Node

- the integer 0 as a flag to indicate this is a leaf node
- the number of data entries in the node
- the serialized representation of each data entry in the node, in order.
  - value of data entry
  - number of rids
  - [p, t] for rid.....

## Configs

### index_info.txt

```
RelationName AttributeName isClusteredFlag TreeOrder
Sailors A 0 10
```

### interpreter_config_file.txt

- first: input directory
- second: output directory
- third: temporary sort directory
- fourth: should build indexes (0 = no, 1 = yes)
- fifth: should evaluate the SQL queries (0 = no, 1 = yes).

### plan_builder_config.txt

- first - Join algorithms:

  - `0`: TNLJ
  - `1 x`: BNLJ with buffer size as x page
  - `2`: SMJ

- second - sort algorithms:

  - `0`: In-Memory Sort
  - `1 x`: External Sort with buffer size as x page

- third - use index:

  - `0`: use the full-scan implementation
  - `1`: use indexes
