/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.pipelines.dnaseq.assembly

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import htsjdk.samtools.{SAMFileHeader, ValidationStringency}
import htsjdk.variant.variantcontext.{VariantContext => HTSJDKVariantContext}
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.common.logging.HlsUsageLogging
import io.projectglow.pipelines.dnaseq.models.LocatableRegion
import io.projectglow.pipelines.dnaseq.util.{BandingVariantContextIterator, IntervalUtil}
import io.projectglow.pipelines.sql.HLSConf
import io.projectglow.vcf.{VCFSchemaInferrer, VariantContextToInternalRowConverter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hls.dsl.expressions.bin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.models.ReadGroupDictionary
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.rdd.read.AlignmentDataset
import org.bdgenomics.adam.sql.Alignment
import org.broadinstitute.hellbender.tools.walkers.haplotypecaller.{HaplotypeCaller, HaplotypeCallerArgumentCollection, ReferenceConfidenceMode => Mode}
import org.broadinstitute.hellbender.utils.{GenomeLocParser, GenomeLocSortedSet, IntervalUtils, SimpleInterval}
import org.broadinstitute.hellbender.utils.read.{GATKRead, SAMRecordToGATKReadAdapter}

/**
 * A utility for running the GATK Assembly tool on Spark
 *
 * The open-source GATK uses a SparkSharder to create shards of reads
 * in Spark, which the HaplotypeCaller then assembles over. This code has
 * several limitations, but the most important of which is that the results it
 * produces are not concordant with the results produced by the GATK.
 *
 * For this reason, we implement this class which is responsible for parallelizing the
 * GATK Assembly tool on top of Spark.
 *
 * The algorithm for parallelizing the GATK assembly is as follows:
 * * We collect all the reads that overlap a given site into the same partition
 *   so that they can be processed together. This is done by binning the genomic range and
 *   range partitioning by the binId.
 * * We also sort the assembly reads within a given partition by [contig, start, end] so that
 *   the reads pileup in order.
 * * Once the reads are sorted, we iterate through the partitions to determine the regions
 *   that we should call on. GATK requires to know upfront what regions it should process.
 * * We then construct an AssemblyRegionIterator from each MultiIntervalShard.
 * * Lastly, we transform the [[HTSJDKVariantContext]] into [[DataFrame]]
 */
object SparkAssembler {

  /**
   * Shards the aligned reads and calls variants within each shard using GATK's
   * [[org.broadinstitute.hellbender.tools.walkers.haplotypecaller.HaplotypeCallerEngine]]
   * and returns a [[DataFrame]] constructed from [[HTSJDKVariantContext]] representing the results
   * of calling variants on the supplied [[AlignmentDataset]]
   *
   * The following invariants are maintained:
   *
   * * Reads that overlap a given site should appear in the same partition
   * * Reads should not be duplicated within the same partition
   * * Reads should be padded by an amount so that nearby reads appear within the same partition
   *
   * The algorithm that implements these invariants works as follows:
   * * We bin each padded read [start - padding, end + padding) into bins of moderate
   *   size ~ O(10000)
   * * This ensures overlapping reads appear in the same bin
   * * This also ensures padded reads appear in the same bin
   * * We group the dataset by contigName, binId
   * * This ensures all the reads for a given bin appear together
   * * Within each group, we invoke the [[AssemblyShardCaller]] instance supplied.
   *
   * @param reads An RDD of aligned reads
   * @param optTargets optionally, the target regions to which we should restrict the calls
   *
   * @return
   */
  private[pipelines] def shardAndCallVariants(
      reads: AlignmentDataset,
      shardCallerFactory: AssemblyShardCallerFactory,
      optTargets: Option[FeatureDataset] = None,
      optExclusions: Option[FeatureDataset] = None): DataFrame = {

    val sparkSession = reads.dataset.sparkSession
    val sc = sparkSession.sparkContext
    val contigToLengthMap = reads
      .sequences
      .records
      .map { sr =>
        sr.name -> sr.length
      }
      .toMap
    val bcContigToLengthMap = sc.broadcast(contigToLengthMap)
    val (header, _) = reads.convertToSam()
    val rgd = reads.readGroups

    import reads.dataset.sqlContext.implicits._
    val targetRegions = optTargets.map(_.dataset.as[LocatableRegion].collect().toList)
    val excludedRegions = optExclusions
      .map(_.dataset.as[LocatableRegion].collect().toList)
      .getOrElse(List.empty)
    val binSize = SQLConf.get.getConf(HLSConf.ASSEMBLY_REGION_BIN_SIZE)
    // Each bin is padded doubly with:
    // - The maximum size of an assembly region, so that assembly regions may cross bin boundaries
    // - The default assembly region padding, so that assembly regions include context
    val binPadding = HaplotypeCaller.DEFAULT_MAX_ASSEMBLY_REGION_SIZE
    val contextPadding = HaplotypeCaller.DEFAULT_ASSEMBLY_REGION_PADDING

    val trimBins = udf { (contigName: String, binStart: Int) =>
      val length = bcContigToLengthMap.value(contigName)
      binStart < length
    }

    val numPartitions = SQLConf.get.getConf(HLSConf.ASSEMBLY_REGION_NUM_PARTITIONS)

    val partitionedReads = reads
      .dataset
      .select(
        struct($"*").alias("alignment"),
        bin($"start" - binPadding - contextPadding, $"end" + binPadding + contextPadding, binSize)
      )
      .withColumn("binStart", $"binId" * binSize)
      .withColumn("binEnd", ($"binId" + 1) * binSize)
      .filter(trimBins($"alignment.referenceName", $"binStart"))
      .repartition(numPartitions, $"alignment.referenceName", $"binId")
      .sortWithinPartitions(
        $"alignment.referenceName",
        $"binId",
        $"alignment.start",
        $"alignment.readName",
        $"alignment.readInFragment"
      )
      .as[BinnedAlignment]

    val caller = shardCallerFactory.getCaller(header)
    val vcfHeader = caller.getVCFHeader
    val mode = caller.referenceConfidenceMode

    val vcfSchema = VCFSchemaInferrer.inferSchema(true, true, vcfHeader)
    val bcVcfHeader = sc.broadcast(vcfHeader)
    val bcVcfSchema = sc.broadcast(vcfSchema)

    val internalRowRdd = partitionedReads.rdd.mapPartitions { iter =>
      // TODO(hhd): We might want to make interval padding a user-provided parameter
      val variantIter = new AssemblyCallerIterator(
        header,
        rgd,
        binPadding,
        intervalPadding = 0,
        targetRegions,
        excludedRegions,
        iter.buffered,
        shardCallerFactory.getCaller
      )
      convertCalls(
        bcVcfHeader.value,
        bcVcfSchema.value,
        variantIter,
        mode
      )
    }

    SQLUtils.internalCreateDataFrame(
      sparkSession,
      internalRowRdd,
      vcfSchema,
      isStreaming = false
    )
  }

  private def convertCalls(
      header: VCFHeader,
      schema: StructType,
      variants: Iterator[HTSJDKVariantContext],
      referenceConfidenceMode: Mode): Iterator[InternalRow] = {
    val bandSpec = new HaplotypeCallerArgumentCollection().GVCFGQBands.asScala.map(n => n: Int)
    val baseIter = if (referenceConfidenceMode == Mode.GVCF) {
      new BandingVariantContextIterator(header, bandSpec, variants)
    } else {
      variants
    }

    val converter =
      new VariantContextToInternalRowConverter(header, schema, ValidationStringency.STRICT)
    baseIter.map(converter.convertRow(_, false))
  }
}

private[assembly] class AssemblyCallerIterator(
    header: SAMFileHeader,
    rgd: ReadGroupDictionary,
    binPadding: Int,
    intervalPadding: Int,
    targetRegions: Option[List[LocatableRegion]],
    excludedRegions: List[LocatableRegion],
    assemblyReads: BufferedIterator[BinnedAlignment],
    makeCallerFn: SAMFileHeader => AssemblyShardCaller)
    extends Iterator[HTSJDKVariantContext]
    with HlsUsageLogging {

  var variantIdx = 0
  val variants = new ArrayBuffer[HTSJDKVariantContext]()
  private lazy val caller = makeCallerFn(header)
  private lazy val converter = new AlignmentConverter()

  override def hasNext(): Boolean = {
    // It's possible that the next bin has no variants in it, so keep trying until we find a bin
    // with variants or exhaust the reads iterator
    while (variantIdx >= variants.size && assemblyReads.hasNext) {
      fillVariantBuffer()
    }
    val ret = variantIdx < variants.size
    if (!ret) {
      logger.info("Iterator is exhausted; shutting down variant caller")
      caller.destroy()
    }
    ret
  }

  override def next(): HTSJDKVariantContext = {
    val idx = variantIdx
    variantIdx += 1
    variants(idx)
  }

  private def assembleBinIntoShard(
      assemblyReads: Seq[GATKRead],
      header: SAMFileHeader,
      contigName: String,
      binStart: Int,
      binEnd: Int,
      binPadding: Int,
      intervalPadding: Int,
      targetRegions: Option[List[LocatableRegion]],
      excludedRegions: List[LocatableRegion]): AssemblyShard = {
    val start = binStart + 1
    val sd = header.getSequenceDictionary
    val length = sd.getSequence(contigName).getSequenceLength
    val end = Math.min(binEnd, length)
    // each bin represents an interval
    // truncate the end of the interval by the contig length to avoid overflow
    val callableInterval = new SimpleInterval(contigName, start, end)
    // each bin is expanded to allow for assembly regions to cross boundaries
    val assemblyStart = Math.max(1, start - binPadding)
    val assemblyEnd = Math.min(end + binPadding, length)
    val interval = new SimpleInterval(contigName, assemblyStart, assemblyEnd)
    val genomeLocParser = new GenomeLocParser(sd)
    val targetIntervals = IntervalUtil
      .trim(interval, targetRegions)
      .map(genomeLocParser.createGenomeLoc)
      .asJava
    val excludedIntervals = excludedRegions
      .map(region => genomeLocParser.createGenomeLoc(region.asInterval))
      .asJava
    val intervals = new GenomeLocSortedSet(genomeLocParser, targetIntervals)
      .subtractRegions(new GenomeLocSortedSet(genomeLocParser, excludedIntervals))
      .toList

    // apply a fixed padding on either side of the unpadded intervals to account for context
    // the invariants imply padded reads are already included in the values
    val paddedIntervals =
      IntervalUtils.getIntervalsWithFlanks(genomeLocParser, intervals, intervalPadding)

    AssemblyShard(
      caller.transformReads(assemblyReads.iterator).asJava,
      callableInterval,
      intervals.asScala.map(loc => new SimpleInterval(loc)).asJava,
      paddedIntervals.asScala.map(loc => new SimpleInterval(loc)).asJava
    )
  }

  private def shouldTakeNextRead(prevRead: BinnedAlignment, head: BinnedAlignment): Boolean = {
    if (prevRead == null) {
      return true
    }

    prevRead.alignment.referenceName == head.alignment.referenceName && prevRead.binId == head.binId
  }

  private def fillVariantBuffer(): Unit = {
    variants.clear()
    variantIdx = 0

    val readsBuffer = new ArrayBuffer[GATKRead]
    var prevRead: BinnedAlignment = null
    while (assemblyReads.hasNext && shouldTakeNextRead(prevRead, assemblyReads.head)) {
      val currentRead = assemblyReads.next()
      prevRead = currentRead
      readsBuffer += new SAMRecordToGATKReadAdapter(
        converter.convert(currentRead.alignment.toAvro, header, rgd)
      )
    }

    if (readsBuffer.nonEmpty) {
      // process buffer
      val assemblyShard = assembleBinIntoShard(
        readsBuffer,
        header,
        prevRead.alignment.referenceName.orNull,
        prevRead.binStart,
        prevRead.binEnd,
        binPadding,
        intervalPadding,
        targetRegions,
        excludedRegions
      )
      val region = assemblyShard.callableRegion
      logger.info(
        s"Calling variants on ${readsBuffer.size} reads in interval " +
        s"(${region.getContig}, ${region.getStart}, ${region.getEnd})"
      )
      variants ++= caller.call(assemblyShard)
    }
  }
}

case class BinnedAlignment(binId: Int, binStart: Int, binEnd: Int, alignment: Alignment)
