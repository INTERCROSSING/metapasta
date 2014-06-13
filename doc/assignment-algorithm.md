### MEGAN-like taxonomy node assignment algorithm

For each read:

1. filter all hits with bitscore below the threshold s_0
2. find the best bitscore value S (in the set of BLAST HSPs corresponding to hits of the read)
3. filter all reads with bitscore below p * S (where p is fixed coefficient, e.g. 0.9)
4. now there are two cases:
    * *rest hits form a line in the taxonomy tree*. In this case we should choose the most specific tax id
    * in other cases we should calculate LCA
    
5. (not for now, questionable) discard leafs with small assignments. 