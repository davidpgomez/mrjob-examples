# mrjob-examples

This repo contains some examples of map-reduce jobs using mrjob library.
## count_edges
From a graph described by its edges (in a plain `txt` file), this job return each edge of the graph with a pair representing the degree of each node the edge joins.
### Input
```
A,B
A,C
B,C
D,E
C,E
D,A

```
### Output
```
["A", "B"]	[3, 2]
["A", "C"]	[3, 3]
["D", "E"]	[2, 2]
["B", "C"]	[2, 3]
["D", "A"]	[2, 3]
["C", "E"]	[3, 2]

```
