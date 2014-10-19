## metapasta usage


### installation

#### nisperoCLI

The most convinient way to work with metapasta is [nisperoCLI](https://github.com/ohnosequences/nisperoCLI). Follow instructions to on its page to install it

#### SBT

SBT version > 0.13 should be installed: http://www.scala-sbt.org/

##### AWS credentials

nisperoCLI tries to resolve credentials from:

* file `~/metapasta.credentials` (path to credentials file can be specified as the last argument to nisperoCLI)
* enviroment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
* instance profile credentials (so it means that everything will work automatically on instances with right IAM role)

To configure your `~/metapasta.credentials` just put these lines there

```bash
accessKey = <access_key>
#for example: accessKey = AKIAIG23IDH2AEPBEFVE

secretKey = <secret_key>
#for example: secretKey = AZpGhgq6i4+m+TRXJ0W8nYmRJY3eqr5p5DQULTci
```

#### configuration of AWS account

To use metapasta your AWS account should be configured.

```
nispero configure
```

### creating template

To download a template with metapasta project type:

```
nispero create ohnosequences/metapasta.g8
```

### configuration

After downloading the template, all metapasta parameters can be installed.

#### samples
Reads should be either single or paired ended with intersection. Example for an mock community:

```scala
object mockSamples {
  val testBucket = "metapasta-test"

  val ss1 = "supermock3"
  val s1 = PairedSample(ss1, ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"), ObjectAddress(testBucket, "mock/" + ss1 + ".fastq"))

  val samples = List(s1)
  
  val t1 = SampleTag("t1")
  val tagging = Map(s1 -> List(t1))
}
```

> FASTQ files should have solexa quality format.

#### configuration
Configuration depends on mapping tool (BLAST or LAST). Some useful parameters:

##### mappingWorkers  
configuration of auto scaling group with mapping workers. It is recommended to use `c1_medium` for BLAST and `m1_large` for LAST. In general BLAST performs mapping quite slow, so for it you will need hundred of mapping workers. Example:

```scala
mappingWorkers = Group(size = 1, max = 100, instanceType = InstanceType.c1_medium, purchaseModel = SpotAuto)
```

LAST requires not so a lot instances but they should have at least 3GB RAM (for nt.16S database).

##### keyName
Name of ssh key that can be used for connecting to instances.

##### timeout
Global timeout in seconds for metapasta

##### database
Index of reference database. Metabasta bundled with nt.16s database, but other databased can be implemented (see https://github.com/ohnosequences/metapasta/blob/master/src/main/scala/ohnosequences/metapasta/Database.scala).


##### other parameters
https://github.com/ohnosequences/metapasta/blob/master/src/main/scala/ohnosequences/metapasta/MetapastaConfiguration.scala



### commands 

##### publish

```
sbt publish
```


##### launching and adding tasks

```
sbt "run run"
```


##### termination

*metapasta* will automatically when all assignments work will finish. Although it is possible to do manual undeploy in case if autotermination doesn't work

```
sbt "run undeploy"
```

in case if it doesn't work:

```
sbt "run undeploy force"
```

##### adding tasks

```
sbt "run add tasks"
```

##### changing size of workers group

```
sbt "run map size <number>"
```






