# LAST mapping tool

[LAST](http://last.cbrc.jp/) is a general purpose mapping tool that we are going to use in the *metapasta*. 
Main advantages of it in compare to BLAST are:

* performance (at least in 100 times faster)
* configurable alignment scoring schemes 

## Parameters

### m
```
Maximum multiplicity for initial matches.  Each initial match is
lengthened until it occurs at most this many times in the
reference.
```

**default:** 10

This parameters is critical when the reference contains a lot of repetitions, for example nt.16S database. Because length of reads are bounded LAST will never find initial matches for read that occurs in more than *m* reference sequences. So this parameter should be big enough to find initial matches for any read from queue, but in same time too big value can decrease performance.

