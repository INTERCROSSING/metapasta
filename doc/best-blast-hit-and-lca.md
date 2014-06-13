### Best blast hit and LCA results in the same configuration

The idea is to use two assignment paradigm in the same time. That means that metapasta will produce two sets of results and use two sets of results, fate logs etc. It is not so easy to implement and adds complexities, but without this feature, user have to run metapasta twice to get lca and bbh results that means that mapping part will be executed also twice. 
Another way to do it is reuse somehow of mapping results.
