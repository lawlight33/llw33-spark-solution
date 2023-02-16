## Table of contents

* [ Algorithms ]
* [ Performance ]
* [ Unit tests ]
* [ Cross-equals check ]
* [ How to run ]

### Algorithms

1. GIT BRANCH NAME: **group-by**
    - make PairRDD by company
    - `groupByKey(company)`
    - `sortBy(number).over(company)`
    - iterate over each company row, enrich with prev number & value
    - `sortBy(number).overAll()`


2. GIT BRANCH NAME: **aggregate**
    - make PairRDD by company
    - `aggregateByKey(company)`.
        - collect lineage for each company within each partition. `CompanyLineage::addRecord`
        - merge partition company lineages. `CompanyLineage::merge`
        - keep lineage sorted
    - enrich each collected lineage for each company with prev number & prev value. `CompanyLineage::compile`
    - `sortBy(number).overAll()`


3. GIT BRANCH NAME: **sorted-partitions**
    - make PairRDD by company_number
    - `repartitionAndSortWithinPartitions()`
        - put company data inside one partition (multiple companies inside partition is also ok)
        - sort each partition by number (not considering company)
    - `mapPartition()`
        - iterate over elements within each partition
        - prepare new enriched partition with prev number & prev value
    - `sortByKey(number)`

### Performance

Each solution was tested on AWS EMR 6.7.0 cluster with S3 as datasource.

AWS EMR Spark cluster spec: 1x driver, 2x executors. VM spec: 4x core, 16 GB x RAM.

Used 20-million post-processed dataset from Kaggle: daily-historical-stock-prices-1970-2018.

Prepared 2 files, each file contains 10 million rows.

| group-by | aggregate | sorted-partitions     |
|----------|-----------|-----------------------|
| 3 min    | 5 min     | 3 min (16 partitions) |

### Unit tests

1. For group-by solution / branch:
    - `SolutionTest::demoTestInput()` for given input
    - `SolutionTest::enrichTest()` for enrich function

2. For aggregate solution / branch:
    - `SolutionTest::demoTestInput()` for given input
    - `SolutionTest::companyLineageTest()` for company linage class (CompanyLineage)

3. For sorted-partitions solution / branch:
    - `SolutionTest::demoTestInput()` for given input
    - `SolutionTest::enrichPartitionTest()` for enrich partition map function
    - `CompanyNamePartitionerTest::partitionTest()` for company Partitioner class
    - `CompanyNumberComparatorTest::compareTest()` for company Comparator class

`demoTestInput()` in each branch use given input:
```
1,APPLE,2000,0,0
2,GOOGLE,10,0,0
3,MICROSOFT,5000,0,0
4,APPLE,100,1,2000
...
```

### Cross-equals check

All results passed cross-equals check:
```
result1DF.exceptAll(result2DF).count() == 0   <---- true for each result pair
````

### How to run
1. Choose wanted algorithm by checkout appropriate git branch.
2. If running with AWS EMR cluster: uncomment AWS EMR sections in pom.xml.
3. Execute: `spark-submit ... com.epam.AppRunner spark-solution.jar "inputDir" "outputDir"`