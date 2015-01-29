MVD
===

Model Variable Development

This is a demo ~~Big~~ Data Application project with [SMV](https://github.com/TresAmigosSD/SMV)

Install SMV
===========

* Install [Spark](http://spark.apache.org/) version 1.02 
* Download [SMV](https://github.com/TresAmigosSD/SMV) with ```git clone```
* Compile SMV with ```mvn clean package install```

Setup MVD
========

* Download [MVD](https://github.com/TresAmigosSD/MVD) with ```git clone```

Get Data
=======

* Download data from 
  http://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Physician-and-Other-Supplier.html
* Unzip the data package
* Link Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt to data/cms/input/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt
* Remove the 2nd line in the file:
```shell
sed -e '2d' Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt > cms_raw_no_2nd_line.csv
```

Run Demos
========

* Discover Schema
```shell
$ mvn package
$ spark-submit --master local[2] --class org.tresamigos.mvd.projectcms.adhoc.DiscoverSchema target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar
```
A directory "cms_raw_no_2nd_line.schema" should be generated under data/cms/input. Move the "part-00000" file out of that directory and rename the file
```shell
$ mv cms_raw_no_2nd_line.schema/part-00000 .
$ rm -rf cms_raw_no_2nd_line.schema
$ mv part-00000 cms_raw_no_2nd_line.schema
```
Review the schema file, and make the following changes
```text
npi: String
bene_unique_cnt: Float
bene_day_srvc_cnt: FLoat
```

* Create CmsRaw
```shell
DATA_DIR=./data/cms spark-submit --master local[2] --class org.tresamigos.mvd.projectcms.core.CmsApp target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar -d org.tresamigos.mvd.projectcms.phase1.CmsRaw
```

* Basic Aggregation 
```shell
DATA_DIR=./data/cms spark-submit --master local[2] --class org.tresamigos.mvd.projectcms.core.CmsApp target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar -d org.tresamigos.mvd.projectcms.adhoc.Ex01SimpleAggregate
```

