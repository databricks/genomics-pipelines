#!/usr/bin/env bash

set -ex
set -o pipefail

# pick up user-provided environment variables 
source /databricks/spark/conf/spark-env.sh

# Install HLS-specific whls (currently only ADAM 0.32.0)
find /mnt/dbnucleus/lib/python -maxdepth 1 -type f -exec /databricks/python/bin/pip install --no-deps {} \;

# Set up Hail
if [[ "$ENABLE_HAIL" == "true" ]]; then
  /databricks/init/setup_hail.sh
fi

# reread user-provided environment variables in case they were modified
source /databricks/spark/conf/spark-env.sh

if [[ -n "$REF_GENOME_PATH" ]]; then
  # Set up custom reference genome, then exit to skip prebuilt reference logic
  /databricks/init/prepare_reference "$REF_GENOME_PATH"
  exit 0
elif [[ "${refGenomeId,,}" == "none" ]]; then
  exit 0
elif [[ "$refGenomeId" == "grch37" ]]; then
  refGenomeName=human_g1k_v37
elif [[ "$refGenomeId" == "grch38" ]]; then
  refGenomeName=GRCh38_full_analysis_set_plus_decoy_hla
fi

# download reference data
aws=/databricks/python2/bin/aws
source /databricks/init/setup_mount_point_creds.sh $MOUNT_POINT
refGenomePath="$DBNUCLEUS_HOME/dbgenomics/$refGenomeId"
$aws configure set default.s3.max_concurrent_requests 100
$aws configure set default.s3.multipart_chunksize 16MB
$aws configure set default.s3.max_queue_size 10000
time $aws s3 sync "$MOUNT_ROOT/$REF_GENOME_PARENT/$refGenomeId" "$refGenomePath"

# Unpack cached VEP database
if [[ "$refGenomeId" == *vep* ]]; then
  cd $refGenomePath
  for a in $(ls -1 *.tar.gz)
  do
    tar -zxvf $a
  done
fi
