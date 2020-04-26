package com.bakkentechnologies

import scalaj.http.Http
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, lit, sum, to_timestamp, udf, when, year}


object ETL {
  def extract(spark: SparkSession, report: String): DataFrame = {
    val df: DataFrame = spark.sqlContext
      .read
      .option("header", "true")
      .csv(s"reports/$report")
    df
  }

  def transform(extracted: DataFrame): DataFrame = {
    val nokDebit = udf((date: String) => {
      val input = Http(s"https://api.exchangeratesapi.io/${date}?base=GBP&symbols=NOK").asString.body
      // println(input)

      val debit: Float = input.split("\"NOK\":")(1).split('}')(0).toFloat
      // println(debit)

      debit
    })

    val df: DataFrame = extracted
      .withColumn("Year",
        year(to_timestamp(col("Date and time (UTC)"), "dd/MM/yyyy HH:mm:ss")))
      .withColumn("Date",
        date_format(to_timestamp(col("Date and time (UTC)"), "dd/MM/yyyy HH:mm:ss"), "yyyy-MM-dd"))
      .withColumn("Exchange Rate",
        lit(nokDebit(col("Date"))))
      .withColumn("Debit (NOK)", when(
        col("Debit") =!= "null",
        lit(col("Debit") * col("Exchange Rate"))
      ))
      .withColumn("Credit (NOK)", when(
        col("Credit") =!= "null",
        lit(col("Credit") * col("Exchange Rate"))
      ))
    df
  }

  def load(transformed: DataFrame, taxYear: String): Unit = {
    val dfTransactions: DataFrame = transformed.where(col("Year") <= taxYear)
    val dfDividends: DataFrame = transformed.where(col("Year") === taxYear)
    val properties: DataFrame = dfTransactions.select("Property Name").distinct()

    properties.collect().foreach { row =>
      val property = row.mkString

      if (!property.equals("null")) {
        val dfPropertyTransactions: DataFrame = dfTransactions.where(col("Property Name").contains(row.mkString))
        val dfPropertyDividends: DataFrame = dfDividends.where(col("Property Name").contains(row.mkString))

        val dividends: DataFrame = dfPropertyDividends.where(col("Transaction Type").contains("Dividend"))
        dividends.select("Date and time (UTC)", "Credit", "Credit (NOK)").show()

        val transactions: DataFrame = dfPropertyTransactions.where(col("Transaction Type").contains("Resale market"))
        transactions.select("Date and time (UTC)", "Units", "Avg Trade Price", "Debit", "Debit (NOK)").show()

        println(row.mkString)
        try { println(s"Units: ${transactions.agg(sum("Units")).first.getDouble(0).floor.toInt}")
        } catch { case _: Throwable => println("No units found.") }
        try { println(s"Values: ${transactions.agg(sum("Debit (NOK)")).first.getDouble(0).floor.toInt} kr")
        } catch { case _: Throwable => println("No values found.") }
        try { println(s"Dividends: ${dividends.agg(sum("Credit (NOK)")).first.getDouble(0).floor.toInt} kr")
        } catch { case _: Throwable => println("No dividends found.") }
        println()
      }
    }
  }
}
