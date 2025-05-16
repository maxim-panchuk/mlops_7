import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object DataMart {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataMart")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.jars", "/app/clickhouse-jdbc-0.4.6-shaded.jar")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val pathToCsv = "/app/en.openfoodfacts.org.products.csv"
    val clickhouseUrl = "jdbc:clickhouse://clickhouse:8123/default"
    val clickhouseUser = "mluser"
    val clickhousePassword = "superpass"
    val clickhouseDriver = "com.clickhouse.jdbc.ClickHouseDriver"
    val clickhouseTable = "food_features_preprocessed"

    val rawDF = spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(pathToCsv)

    val features = Seq(
      "energy-kcal_100g", 
      "fat_100g", 
      "saturated-fat_100g", 
      "carbohydrates_100g", 
      "sugars_100g", 
      "proteins_100g", 
      "fiber_100g", 
      "salt_100g"
    )

    val dfSelected = rawDF.select(features.map(col): _*)

    val dfNoNulls = dfSelected.na.drop()

    val dfLimited = dfNoNulls.limit(300000)

    val threshold = 100
    val maxKcal = 900

    val columnsToCheck = features.filter(_ != "energy-kcal_100g")
    val filterCondition = columnsToCheck.map(c => col(c) < threshold).reduce(_ && _)
    val energyCondition = col("energy-kcal_100g") < maxKcal
    val positiveCondition = features.map(c => col(c) > 0).reduce(_ && _)
    val combinedFilter = filterCondition && energyCondition && positiveCondition
    val dfFiltered = dfLimited.filter(combinedFilter)

    dfFiltered.write
      .format("jdbc")
      .option("url", clickhouseUrl)
      .option("dbtable", clickhouseTable)
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("driver", clickhouseDriver)
      .mode(SaveMode.Overwrite)
      .save()

    spark.stop()
    println("DataMart job finished. Preprocessed data saved to ClickHouse.")
  }
}
