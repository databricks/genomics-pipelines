# Regenerates GATK golden files for comparison upon GATK version changes

DESIRED_GATK_VERSION=$(~/gatk/gatk --version | grep GATK | sed -E 's/.*v([0-9.]*).*/\1/')

# Checks if the file was already generated with the same tool and GATK version

file_has_desired_gatk_version () {
  FILE_NAME=$1
  TOOL_NAME=$2

  if [ -f $FILE_NAME ]; then
    GATK_HEADER_PATTERN="##GATKCommandLine=<ID=$TOOL_NAME"
    FILE_GATK_HEADER_LINE=$(grep -m 1 "$GATK_HEADER_PATTERN" $FILE_NAME)
    FILE_GATK_VERSION=$(sed -E 's/.*Version="([0-9.]*).*/\1/' <<< $FILE_GATK_HEADER_LINE)

    if [ "$DESIRED_GATK_VERSION" == "$FILE_GATK_VERSION" ]; then
      echo "$FILE_NAME was generated with $TOOL_NAME GATK version $DESIRED_GATK_VERSION"
      return 0
    else
      if [ -z "$FILE_GATK_HEADER_LINE" ]; then
        echo "$FILE_NAME was not generated with $TOOL_NAME"
        return 1
      else
        echo "$FILE_NAME was generated with $TOOL_NAME GATK version $FILE_GATK_VERSION"
        return 1
      fi
    fi
  else
    echo "$FILE_NAME does not exist"
    return 1
  fi
}

# DNASeq

TOOL=HaplotypeCaller

OUTPUT=dnaseq/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
    ~/gatk/gatk $TOOL \
      --input ~/gatk/src/test/resources/large/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam \
      --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
      --max-reads-per-alignment-start 0 \
      --output $OUTPUT
fi

OUTPUT=dnaseq/NA12878_21_10002403.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input NA12878_21_10002403.bam \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=dnaseq/NA12878_21_10002403.g.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input NA12878_21_10002403.bam \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --emit-ref-confidence GVCF \
    --intervals 21_10002393_10002413.bed \
    --output $OUTPUT
fi

OUTPUT=dnaseq/NA12878_21_10002403.bp.g.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input NA12878_21_10002403.bam \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --emit-ref-confidence BP_RESOLUTION \
    --intervals 21_10002393_10002413.bed \
    --output $OUTPUT
fi

OUTPUT=dnaseq/aligned.reads.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input aligned.reads/bwa_mem_paired_end.bam \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

# MutSeq

TOOL=Mutect2

OUTPUT=mutseq/golden_1.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/normal_1.bam \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/tumor_1.bam \
    --exclude-intervals mutseq/mask1.bed \
    --normal-sample "synthetic.challenge.set1.normal" \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=mutseq/golden_2.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/normal_2.bam \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/tumor_2.bam \
    --exclude-intervals mutseq/mask2.bed \
    --normal-sample "synthetic.challenge.set2.normal" \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=mutseq/golden_3.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/normal_3.bam \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/tumor_3.bam \
    --exclude-intervals mutseq/mask3.bed \
    --normal-sample "G15512.prenormal.sorted" \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=mutseq/golden_4.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk $TOOL \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/normal_4.bam \
    --input ~/gatk/src/test/resources/large/mutect/dream_synthetic_bams/tumor_4.bam \
    --exclude-intervals mutseq/mask4.bed \
    --normal-sample "synthetic.challenge.set4.normal" \
    --reference ~/gatk/src/test/resources/large/human_g1k_v37.20.21.fasta \
    --output $OUTPUT
fi

# Joint genotyping

TOOL=GenotypeGVCFs
COMBINED_OUTPUT=joint/combined.g.vcf

OUTPUT=joint/genotyped.chr20_419.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk CombineGVCFs \
    --variant joint/HG002.chr20_419.g.vcf \
    --variant joint/HG003.chr20_419.g.vcf \
    --variant joint/HG004.chr20_419.g.vcf \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $COMBINED_OUTPUT

  ~/gatk/gatk $TOOL \
    --allow-old-rms-mapping-quality-annotation-data \
    --variant $COMBINED_OUTPUT \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=joint/genotyped.chr20_17960111.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk CombineGVCFs \
    --variant joint/HG00096.chr20_17960111.g.vcf \
    --variant joint/HG00268.chr20_17960111.g.vcf \
    --variant joint/NA19625.chr20_17960111.g.vcf \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $COMBINED_OUTPUT

  ~/gatk/gatk $TOOL \
    --allow-old-rms-mapping-quality-annotation-data \
    --variant $COMBINED_OUTPUT \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=joint/genotyped.chr20_17983267.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk CombineGVCFs \
    --variant joint/HG00096.chr20_17983267.g.vcf \
    --variant joint/HG00268.chr20_17983267.g.vcf \
    --variant joint/NA19625.chr20_17983267.g.vcf \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $COMBINED_OUTPUT

  ~/gatk/gatk $TOOL \
    --allow-old-rms-mapping-quality-annotation-data \
    --variant $COMBINED_OUTPUT \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=joint/genotyped.chr20_18034651_18034655.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk CombineGVCFs \
    --variant joint/HG00096.chr20_18034651_18034655.g.vcf \
    --variant joint/HG00268.chr20_18034651_18034655.g.vcf \
    --variant joint/NA19625.chr20_18034651_18034655.g.vcf \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $COMBINED_OUTPUT

  ~/gatk/gatk $TOOL \
    --allow-old-rms-mapping-quality-annotation-data \
    --variant $COMBINED_OUTPUT \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $OUTPUT
fi

OUTPUT=joint/genotyped.chr21_575.vcf
if ! file_has_desired_gatk_version $OUTPUT $TOOL; then
  ~/gatk/gatk CombineGVCFs \
    --variant joint/HG002.chr21_575.g.vcf \
    --variant joint/HG003.chr21_575.g.vcf \
    --variant joint/HG004.chr21_575.g.vcf \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $COMBINED_OUTPUT

  ~/gatk/gatk $TOOL \
    --allow-old-rms-mapping-quality-annotation-data \
    --variant $COMBINED_OUTPUT \
    --reference ~/gatk/src/test/resources/large/Homo_sapiens_assembly38.20.21.fasta \
    --output $OUTPUT
fi

if [ -f $COMBINED_OUTPUT ]; then
  rm $COMBINED_OUTPUT
fi
