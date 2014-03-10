## metapasta usage

### samples
Reads should be either single or paired ended with intersection. Example for mock community:

```scala
object mockSamples {
  val testBucket = "metapasta-test"

  val ss1 = "SRR172902"
  val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

  val ss2 = "SRR172903"
  val s2 = PairedSample(ss2, ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss2 + ".fastq"))

  val samples = List(s1, s2)
}
```

> FASTQ files should have solexa quality format.

### installation

#### cs and g8
[`conscript`](https://github.com/n8han/conscript#installation) and [`g8`](https://github.com/n8han/giter8#installation) should be installed

```
g8 ohnosequences/metapasta
```

#### credentials

credentials should be placed in `~/metapasta.credentials`, the file should have a format like this

```bash
accessKey = AKIAIG23IDH2AEPBEFVA
#for example: accessKey = AKIAIG23IDH2AEPBEFVA

secretKey = AZpGhgq6i4+m+TRXJ0W8nYmRJY3ejr5p5DQULTci
#for example: secretKey = AZpGhgq6i4+m+TRXJ0W8nYmRJY3ejr5p5DQULTci
```

### commands 

##### publish

```
sbt publish
```


##### launch


```
sbt "run run"
```

##### termination

*metapasta* will automatically when all assignments work will finish. Although it is possible to do force undeploy in case if autotermination doesn't work

```
sbt "undeploy force"
```
