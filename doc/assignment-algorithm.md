### Assignment algorithm

1. filter all hits with bitscore below the threshold s_0
2. find the best bitscore value S
3. filter all reads bellow p * S (where p is fixed coefficient, e.g. 0.9)
4. now there are two cases:
    * *rest hits form a line in the taxonomy tree*. In this case we should choose the most specific tax id
    * in other cases we should calculate LCA
5. (not for now, questionable) discard leafs with small assignments. 
