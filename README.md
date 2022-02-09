Genomics pipelines. At scale. With Spark and Glow. :exploding_head:

# What's inside?

Spark based pipelines for:
- Variant calling (built on GATK's HaplotypeCaller)
- Somatic variant calling (built on MuTect2)
- Joint genotyping (built on GenotypeGVCFs)

# Building and testing

1. Clone the repo
2. Unpack the big test files archive located in the project root
  - `tar -xf big-files.tar.gz` 
3. `sbt test`

# Running on a Databricks cluster

1. Create an init script to download the reference genome from cloud storage (see `hls.sh` or
   `prepare_reference.py` for inspiration.
2. Build an uber jar (`sbt assembly`)
3. Create a cluster with the init script from step 1 and attach the assembly jar.
4. Run the desired pipeline using one of the attached notebooks.
