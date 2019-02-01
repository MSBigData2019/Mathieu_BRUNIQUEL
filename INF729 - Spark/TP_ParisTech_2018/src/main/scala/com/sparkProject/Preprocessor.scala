package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Preprocessor {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    import spark.implicits._

    /*******************************************************************************
      *
      *       TP 2
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    println("hello world ! from Preprocessor")

    val df_clean: DataFrame = spark
      .read
      .option("header", true)  // Use first line of all files as header
      .option("inferSchema", "true") // Try to infer the data types of each column
      .csv("/Users/mathieubruniquel/Desktop/MS BGD/3. Cours/INF 729 - Spark/1.TP/TP2/Data/train_clean.csv")

    /* Affichage du nombre de lignes du df_clean
    print("Nombre de lignes : " + df_clean.count()) */

    /* Affichage du nombre de colonnes du df_clean
    print("Nombre de colonnes : " + df_clean.columns.size)  */

    /* Affichage du df_clean
    df_clean.show(10) */

    /* Affichage du schéma du df_clean
    df_clean.printSchema() */


    /* Assignation du type Int aux colonnes concernées */
    import org.apache.spark.sql.types.IntegerType
    val df_clean2: DataFrame = df_clean
      .withColumn("goal", $"goal".cast("Int"))
      .withColumn("backers_count", $"backers_count".cast("Int"))
      .withColumn("deadline", $"deadline".cast("Int"))
      .withColumn("state_changed_at", $"state_changed_at".cast("Int"))
      .withColumn("created_at", $"created_at".cast("Int"))
      .withColumn("launched_at", $"launched_at".cast("Int"))


    /* Affichage description statistique des colonnes de type int
    df_clean2.select("goal", "backers_count").describe().show() */

    /* Exploration des variables
    df_clean2.groupBy("disable_communication").count().orderBy($"count".desc).show()
    df_clean2.groupBy($"keywords").count().orderBy($"count".desc).show()
    df_clean2.groupBy($"country").count().orderBy($"count".desc).show()
    df_clean2.groupBy($"currency").count().orderBy($"count".desc).show()
    /* etc ... */
    */

    // Affichage de la currency des pays "False"
    // df_clean2.filter($"country" === "False").groupBy("currency").count().orderBy($"count".desc).show()


    // Abandon disable_communication //
    val df_clean3: DataFrame = df_clean2.drop("disable_communication")

    // Abandon des "fuites du futur" : backers_count et state_changed_at //
    val df_clean4: DataFrame = df_clean3.drop("backers_count", "state_changed_at")


    // UDF country
    def udfCountry = udf{(country: String, currency: String) =>
      if (country == "False")
        currency
      else
        country
    }

    // UDF currency
    def udfCurrency = udf{(currency: String) =>
      if ( currency != null && currency.length != 3 )
        null
      else
        currency
    }

    val df_country: DataFrame = df_clean4
      .withColumn("country2", udfCountry($"country", $"currency"))
      .withColumn("currency2", udfCurrency($"currency"))
      .drop("country", "currency")

    df_country.groupBy("country2","currency2").count.orderBy($"count".desc).show()

  }

}
