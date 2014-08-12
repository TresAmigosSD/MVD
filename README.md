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

* Download with ```git clone```
* Download data from 
  http://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Physician-and-Other-Supplier.html
* Unzip the data package
* Link Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt to MVD/data/CMS_Data/raw/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt

Run Demos
========

* Compile MVD with ```mvn clean package```
* Run 1st job with Spark

    ```
    ~/spark-1.0.2/bin/spark-submit --class org.tresamigos.mvd.Demo001 --master local[2] target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```

  Result files are under:

    ```
    target/classes/data/out/
    ```

* Data Split and Sampling 

    ```
    ~/spark-1.0.2/bin/spark-submit --class org.tresamigos.mvd.SampleSplit --master local[2] target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```

    This application creates 2 folders under ```MVN/data/CMS_Data/```, ```samples``` and ```all```. Basically the raw data is splited based on a has on the ```nip``` key 
    to multiple partitions and output as gzip files under ```all```, and a 5% sample also on the hash of ```nip``` is created under ```sampled``` directory

* Run Some Basic Data Check

    ```
    ~/spark-1.0.2/bin/spark-submit --class org.tresamigos.mvd.CMS_EDD --master local[2] target/mvd-1.0-SNAPSHOT-jar-with-dependencies.jar
    ```

    Extended Data Dictionary (EDD) report is generated under ```MVN/data/CMS_Data/report/edd/```. 
    You can browse ```part-00000.gz``` for the reprort. Something like this

    ```
    Total Record Count:                        9153272
    npi                  Non-Null Count:        9153272
    npi                  Approx Distinct Count: 876596
    npi                  Min Length:            10
    npi                  Max Length:            10
    nppes_credentials    Non-Null Count:        8595480
    nppes_credentials    Approx Distinct Count: 12682
    nppes_credentials    Min Length:            1
    nppes_credentials    Max Length:            20
    nppes_provider_gender Non-Null Count:        8773306
    nppes_provider_gender Approx Distinct Count: 2
    nppes_provider_gender Min Length:            1
    nppes_provider_gender Max Length:            1
    nppes_entity_code    Non-Null Count:        9153272
    nppes_entity_code    Approx Distinct Count: 3
    nppes_entity_code    Min Length:            1
    nppes_entity_code    Max Length:            1
    ......
    Histogram of average_Medicare_allowed_amt as AMOUNT
    key                      count      Pct    cumCount   cumPct
    0.01                   1153331   12.60%     1153331   12.60%
    10.0                    990219   10.82%     2143550   23.42%
    20.0                    845944    9.24%     2989494   32.66%
    30.0                    650075    7.10%     3639569   39.76%
    40.0                    508568    5.56%     4148137   45.32%
    50.0                    417918    4.57%     4566055   49.88%
    60.0                    697068    7.62%     5263123   57.50%
    70.0                    545009    5.95%     5808132   63.45%
    80.0                    303290    3.31%     6111422   66.77%
    90.0                    389402    4.25%     6500824   71.02%
    100.0                   410361    4.48%     6911185   75.51%
    110.0                   282135    3.08%     7193320   78.59%
    120.0                   203092    2.22%     7396412   80.81%
    130.0                   198263    2.17%     7594675   82.97%
    ......
    ```
  
* By City EDD 
  Similarly, you can try CMS_TopCity class for more fun stuff
    

  
