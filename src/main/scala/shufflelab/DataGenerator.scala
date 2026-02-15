package shufflelab

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


/**
 * Generates ~5M synthetic e-commerce transactions with intentional skew.
 *
 * Schema: order_id, user_id, product_id, category, amount, timestamp
 *
 * 40% of rows are assigned to a single "hot" user_id to create observable
 * partition skew in downstream shuffle operations.
 */
object DataGenerator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataGenerator")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "/opt/spark-events")
      .getOrCreate()

    import spark.implicits._

    val numRows      = 5000000
    val numUsers     = 100000
    val numProducts  = 10000
    val hotUserId    = 9999  // 40% of all rows will belong to this user
    val categories   = Array("Electronics", "Clothing", "Books", "Home", "Sports", "Toys", "Food", "Beauty")

    println(s"Generating $numRows transactions (40% skewed to user_id=$hotUserId)...")

    // Generate data using Spark SQL functions instead of a local collection
    // to avoid blowing out driver heap memory
    val categoriesCol = array(categories.map(lit): _*)
    val numCategories = categories.length

    val df = spark.range(1, numRows.toLong + 1).toDF("order_id")
      .withColumn("_rand", rand(42))
      .withColumn("user_id",
        when(col("_rand") < 0.4, lit(hotUserId))
          .otherwise((rand(43) * numUsers).cast("int"))
      )
      .withColumn("product_id", (rand(44) * numProducts).cast("int"))
      .withColumn("category", element_at(categoriesCol, (rand(45) * numCategories).cast("int") + 1))
      .withColumn("amount", round(rand(46) * 500, 2))
      .withColumn("timestamp", (lit(1700000000L) + (rand(47) * 2592000).cast("int")).cast("long"))
      .drop("_rand")

    val outputPath = "/opt/spark-data/transactions"
    df.repartition(8)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    val count = spark.read.parquet(outputPath).count()
    println(s"Wrote $count rows to $outputPath")

    // Also generate a small products dimension table for join exercises
    val products = spark.range(0, numProducts.toLong).toDF("product_id")
      .withColumn("product_id", col("product_id").cast("int"))
      .withColumn("product_name", concat(lit("Product_"), col("product_id")))
      .withColumn("category", element_at(categoriesCol, (col("product_id") % numCategories) + 1))
      .withColumn("weight_kg", round(rand(48) * 20, 2))

    val productsPath = "/opt/spark-data/products"
    products.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(productsPath)

    println(s"Wrote ${products.count()} products to $productsPath")
    println("Data generation complete.")

    spark.stop()
  }
}
