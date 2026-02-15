package shufflelab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Compares broadcast hash join vs sort-merge join using the same data.
 *
 * The products table is small (~10K rows) so Spark will broadcast it by default.
 * We then force a sort-merge join by lowering the broadcast threshold and compare plans.
 */
object JoinStrategies {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JoinStrategies")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "/opt/spark-events")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE for stable plans
      .getOrCreate()

    import spark.implicits._

    val transactions = spark.read.parquet("/opt/spark-data/transactions")
    val products     = spark.read.parquet("/opt/spark-data/products")

    println(s"Transactions: ${transactions.count()} rows, ${transactions.rdd.getNumPartitions} partitions")
    println(s"Products:     ${products.count()} rows")

    // -------------------------------------------------------------------------
    // Broadcast Hash Join (default — small table is broadcast to all executors)
    // -------------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("  BROADCAST HASH JOIN")
    println("  The small products table is sent to every executor.")
    println("  No shuffle needed on the large transactions table.")
    println("=" * 70)

    // Ensure broadcast threshold is high enough (default 10MB is fine for our small table)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

    val broadcastJoin = transactions.join(products, "product_id")
      .groupBy("category")
      .agg(
        sum("amount").as("total_revenue"),
        avg("weight_kg").as("avg_product_weight"),
        count("*").as("num_orders")
      )

    QueryPlanReader.show("Broadcast Hash Join", broadcastJoin)
    broadcastJoin.show(truncate = false)

    // -------------------------------------------------------------------------
    // Sort-Merge Join (forced by disabling broadcast)
    // -------------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("  SORT-MERGE JOIN")
    println("  Both tables are shuffled and sorted by the join key.")
    println("  More expensive but necessary when both tables are large.")
    println("=" * 70)

    // Set threshold to -1 to disable broadcast joins entirely
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val sortMergeJoin = transactions.join(products, "product_id")
      .groupBy("category")
      .agg(
        sum("amount").as("total_revenue"),
        avg("weight_kg").as("avg_product_weight"),
        count("*").as("num_orders")
      )

    QueryPlanReader.show("Sort-Merge Join", sortMergeJoin)
    sortMergeJoin.show(truncate = false)

    // -------------------------------------------------------------------------
    // Side-by-side comparison
    // -------------------------------------------------------------------------
    QueryPlanReader.compare(
      "Broadcast Hash Join", broadcastJoin,
      "Sort-Merge Join", sortMergeJoin
    )

    println("""
    |  WHAT TO LOOK FOR in the Spark UI:
    |  ----------------------------------
    |  • Broadcast join: only ONE Exchange (shuffle) — just the aggregation
    |    The small table is broadcast, so no shuffle needed for the join itself
    |  • Sort-Merge join: TWO Exchanges — both tables are shuffled by product_id,
    |    then sorted before the merge
    |  • Broadcast is faster for small dimension tables
    |  • Sort-Merge scales to any table size but costs more in shuffle I/O
    |  • Tip: in production, use broadcast hints for tables you KNOW are small:
    |    transactions.join(broadcast(products), "product_id")
    """.stripMargin)

    spark.stop()
  }
}
