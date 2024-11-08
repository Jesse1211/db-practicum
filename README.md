# plan_builder_config.txt

# index_info.txt

```
RelationName AttributeName isClusteredFlag TreeOrder
Sailors A 0 10
```

# interpreter_config_file.txt

- first line: input directory
- second line: output directory
- third line: temporary sort directory
- fourth line: whether the interpreter should build indexes (0 = no, 1 = yes)
- fifth line: whether the interpreter should actually evaluate the SQL queries (0 = no, 1 = yes).

# plan_builder_config.txt

#### first line - Join algorithms:

- `0`: TNLJ
- `1 x`: BNLJ with buffer size as x page
- `2`: SMJ

#### second line - sort algorithms:

- `0`: In-Memory Sort
- `1 x`: External Sort with buffer size as x page

#### third lind - use index:

- `0`: use the full-scan implementation
- `1`: use indexes
