package shufflelab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShuffleExplorer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ShuffleExplorer")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "/opt/spark-events")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE so we can see raw plans
      .getOrCreate()

    import spark.implicits._

    val transactions = spark.read.parquet("/opt/spark-data/transactions")
    transactions.cache()
    transactions.count() // Materialize cache

    val scenario = args.headOption.getOrElse("all")

    if (scenario == "all" || scenario == "1") scenario1_groupByKey_vs_reduceByKey(spark, transactions)
    if (scenario == "all" || scenario == "2") scenario2_repartition_vs_coalesce(spark, transactions)
    if (scenario == "all" || scenario == "3") scenario3_skew_and_salting(spark, transactions)

    println("\nAll scenarios complete. Check the Spark UI for shuffle metrics.")
    spark.stop()
  }

  // ---------------------------------------------------------------------------
  // Scenario 1: groupByKey vs reduceByKey
  // ---------------------------------------------------------------------------
  // groupByKey shuffles ALL values to the reducer before aggregating.
  // reduceByKey (map-side combine) aggregates locally first, reducing shuffle.
  //
  // In the DataFrame API this maps to:
  //   groupByKey-style → groupBy + collect_list (forces full shuffle of values)
  //   reduceByKey-style → groupBy + sum (partial agg before shuffle)
  // ---------------------------------------------------------------------------
  private def scenario1_groupByKey_vs_reduceByKey(
    spark: SparkSession,
    transactions: org.apache.spark.sql.DataFrame
  ): Unit = {
    import spark.implicits._

    println("\n" + "=" * 70)
    println("  SCENARIO 1: groupByKey-style vs reduceByKey-style aggregation")
    println("=" * 70)

    // groupByKey-style: collect all amounts, then sum client-side
    // This forces ALL amount values to shuffle to the reducer
    val groupByKeyStyle = transactions
      .groupBy("user_id")
      .agg(collect_list("amount").as("all_amounts"))
      .select($"user_id", aggregate($"all_amounts", lit(0.0), (acc, x) => acc + x).as("total"))

    // reduceByKey-style: partial aggregation before shuffle
    val reduceByKeyStyle = transactions
      .groupBy("user_id")
      .agg(sum("amount").as("total"))

    QueryPlanReader.compare(
      "groupByKey-style (collect_list → sum)", groupByKeyStyle,
      "reduceByKey-style (partial agg → sum)", reduceByKeyStyle
    )

    // Force full execution — .count() would let Spark skip the actual aggregation
    groupByKeyStyle.write.mode("overwrite").parquet("/opt/spark-data/output/groupby_key_style")
    println("  groupByKey-style written.")

    reduceByKeyStyle.write.mode("overwrite").parquet("/opt/spark-data/output/reduce_key_style")
    println("  reduceByKey-style written.")

    println("""
    |  Expected Results:
    |  -----------------------------------------------
    |  • groupByKey-style has LARGER shuffle write — all amount values cross the network
    |  • reduceByKey-style has SMALLER shuffle write — only partial sums cross the network
    |  • Compare "Shuffle Write" and "Shuffle Read" columns between the two jobs
    """.stripMargin)
  }

  // ---------------------------------------------------------------------------
  // Scenario 2: repartition(n) vs coalesce(n)
  // ---------------------------------------------------------------------------
  // repartition triggers a FULL shuffle (Exchange node in the plan).
  // coalesce avoids a shuffle by merging partitions on the same executor.
  // ---------------------------------------------------------------------------
  private def scenario2_repartition_vs_coalesce(
    spark: SparkSession,
    transactions: org.apache.spark.sql.DataFrame
  ): Unit = {
    println("\n" + "=" * 70)
    println("  SCENARIO 2: repartition vs coalesce")
    println("=" * 70)

    val currentPartitions = transactions.rdd.getNumPartitions
    println(s"  Current partition count: $currentPartitions")

    // Reduce from current count down to 2
    val repartitioned = transactions.repartition(2)
    val coalesced     = transactions.coalesce(2)

    QueryPlanReader.compare(
      "repartition(2) — full shuffle", repartitioned,
      "coalesce(2) — no shuffle", coalesced
    )

    // Force execution by writing — .count() would optimize away the shuffle difference
    repartitioned.write.mode("overwrite").parquet("/opt/spark-data/output/repartitioned")
    println("  repartition(2) written.")

    coalesced.write.mode("overwrite").parquet("/opt/spark-data/output/coalesced")
    println("  coalesce(2) written.")

    println("""
    |  Expected Results:
    |  ----------------------------------
    |  • repartition(2) creates a new stage with an Exchange (shuffle) node
    |  • coalesce(2) does NOT create a new stage — partitions are merged locally
    |  • repartition is useful when you need even data distribution
    |  • coalesce is better when you just want fewer output files
    """.stripMargin)
  }

  // ---------------------------------------------------------------------------
  // Scenario 3: Skew detection and salting
  // ---------------------------------------------------------------------------
  // user_id=9999 has 40% of rows. This causes one partition to be much larger
  // than others during a groupBy, making one task take far longer.
  //
  // Salting adds a random suffix to the key, spreading the hot key across
  // multiple partitions, then aggregates again to combine partial results.
  // ---------------------------------------------------------------------------
  private def scenario3_skew_and_salting(
    spark: SparkSession,
    transactions: org.apache.spark.sql.DataFrame
  ): Unit = {
    import spark.implicits._

    println("\n" + "=" * 70)
    println("  SCENARIO 3: Skew detection and salting fix")
    println("=" * 70)

    // Show the skew
    println("\n  Top 10 users by row count (notice the skew):")
    transactions.groupBy("user_id")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    // Naive aggregation — one partition will be huge
    val naive = transactions
      .groupBy("user_id")
      .agg(sum("amount").as("total"), count("*").as("num_orders"))

    QueryPlanReader.show("Naive groupBy (skewed)", naive)

    // Trigger execution so we can see task duration skew in the UI
    naive.write.mode("overwrite").parquet("/opt/spark-data/output/naive_agg")

    // Salted aggregation — spread hot key across N sub-partitions
    val saltBuckets = 10
    val salted = transactions
      .withColumn("salt", (rand() * saltBuckets).cast("int"))
      .groupBy($"user_id", $"salt")
      .agg(sum("amount").as("partial_total"), count("*").as("partial_count"))
      .groupBy("user_id")
      .agg(sum("partial_total").as("total"), sum("partial_count").as("num_orders"))

    QueryPlanReader.show("Salted groupBy (skew mitigated)", salted)

    salted.write.mode("overwrite").parquet("/opt/spark-data/output/salted_agg")

    println("""
    |  Expected Results:
    |  ----------------------------------
    |  • Naive: one task takes much longer than others (check task duration in Stages)
    |  • Salted: tasks are more evenly distributed
    |  • Salted plan has TWO shuffles (two groupBy stages) but each is more balanced
    |  • The trade-off: extra shuffle overhead vs eliminating the straggler task
    """.stripMargin)
  }
}
