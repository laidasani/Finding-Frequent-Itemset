# Finding-Frequent-Itemset

## Overview
In this project, I implemented **SON Algorithm** using the Apache Spark Framework to find frequent item sets.

One of the major tasks is to find **all the possible combinations of the frequent itemsets** in a given input file using A-Priori algorithms. The project involves working of SON algorithm on two different datasets, one simulated dataset and one real-world generated dataset.

Apart from input file, 2 separate inputs are provided:
•	**Filter threshold:** Integer that is used to filter out qualified users
•	**Support:** Integer that defines the minimum count to qualify as a frequent itemset 

The steps for finding frequent itemset includes: 

1)	Finding the **candidates** of frequent itemset (as singletons, pairs, triples, etc.) that maybe qualified as frequent given a support threshold (that maps to a frequent bucket).

2)	Calculating the combinations of frequent itemset (as singletons, pairs, triples, etc.) that are actually frequent given a support threshold. 

The code is optimized to run efficiently under 500 seconds for support 50 and filter threshold 20. The printed itemsets are sorted in lexicographical order.
