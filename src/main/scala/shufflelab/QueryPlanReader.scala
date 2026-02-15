package shufflelab

import org.apache.spark.sql.DataFrame

/** Utility to print and compare Spark physical plans with highlighted key nodes. */
object QueryPlanReader {

  private val highlightPatterns = Seq(
    "Exchange",            // Shuffle boundary
    "HashAggregate",       // Aggregation operator
    "BroadcastHashJoin",   // Broadcast join
    "SortMergeJoin",       // Sort-merge join
    "BroadcastExchange",   // Broadcast distribution
    "ShuffleExchangeExec"  // Full shuffle
  )

  /** Print the formatted physical plan for a single DataFrame. */
  def show(label: String, df: DataFrame): Unit = {
    println(s"\n${"=" * 70}")
    println(s"  PLAN: $label")
    println("=" * 70)
    df.explain("formatted")
    println()
    highlightNodes(df)
  }

  /** Compare two physical plans side by side (sequentially printed). */
  def compare(labelA: String, dfA: DataFrame, labelB: String, dfB: DataFrame): Unit = {
    show(labelA, dfA)
    show(labelB, dfB)
    println(s"\n${"=" * 70}")
    println("  ^ Compare the two plans above. Look for differences in:")
    println("    - Number of Exchange (shuffle) nodes")
    println("    - Join strategy (BroadcastHashJoin vs SortMergeJoin)")
    println("    - Aggregation strategy (partial vs full HashAggregate)")
    println("=" * 70)
  }

  /** Print which key plan nodes appear in the physical plan. */
  private def highlightNodes(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    val found = highlightPatterns.filter(plan.contains)
    if (found.nonEmpty) {
      println(s"  Key nodes in this plan: ${found.mkString(", ")}")
    }
  }
}
