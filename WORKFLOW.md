Single Data File Work Flow
=========================

Assumption
-----
* Single large data file with one or multiple "account" key(s), which we can sample and/or partition on
* No significant file level quality issues (e.g. partial file, corrupted file, etc.)

Data file preparation for discovery
-----
1. Create Schema file 
1. Estimate number of records 
1. Get the first 10K-100K records

        $> zcat rawdata.gz|head -10000|gzip >raw10k.gz

1. Remove data header

        val raw_rdd = sc.textFile(raw, 1).pipe("sed -e 1d")

1. Run quick DQR on the 10K data
1. Refine Schema based on the quick-DQR
1. Identify quick data fix if needed. Develop quick data fix filters
1. Create a Tiny data with about first 1000 lines 
1. Estimate number of accounts 
1. Determine the sampling size for the Sampled data
1. Split main data to 2^m pieces (sometimes optional)

        val p_rdd = raw_rdd.hashPartition(8, index=2, delimiter='\t')
        p_rdd.saveAsGZFile(alldir)

1. Create a Sampled data with about 1-10% on accounts (random sample), and split the same way as main data

        p_rdd.hashSample(0.05, index=2, delimiter='\t').saveAsGZFile(sampleddir)

After this step, the data directory structure will look like below:

      CMS_Data/
      ├── all/
      ├── sampled/
      ├── tiny/
      └── schema

Could potentially also have the following directories:
* DQM_scripts  -- for data specific DQM and fixing scripts
* rejected  -- for storing data records rejected by the DQM scripts

Model variable discovery and development
-----
1. Run DQR on "Sampled" data
1. Review DQR and cross-check with "Tiny" and/or "Sampled" data to
    1. Detect field level statistical data issues (e.g. high missing rate)
    1. Enhance understanding on data Schema
    1. Collect data questions for further investigation (ask client or search on-line, etc.)
1. Loop through the following steps:
    1. Brainstorming on what variables can be derived and could be potentially useful based on the enhanced understanding of the data fields
    1. Create MVs. Loop through the following steps
        1. Code the MVs 
        1. Run on "Tiny" data
        1. Check results by eyeballing
    1. Run on "Sampled" data
    1. Run DQR on previous step's output
    1. Run multiple correlation test against MV pairs 
    1. Fix/remove wrong MVs, comment out trivial MVs
    1. Create test data and test script
    1. Run test, commit to SVN or Git
1. Run MVs on "All" data
1. Run DQR on "All" data MV results 


