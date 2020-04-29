package com.bakkentechnologies

import scalaj.http.Http
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, first, lit, sum, to_timestamp, udf, when, year}


object ETL {
  def extract(spark: SparkSession, report: String): DataFrame = {
    val df: DataFrame = spark.sqlContext
      .read
      .option("header", "true")
      .csv(s"reports/$report")
    df
  }


  def transform(extracted: DataFrame, currency: String, taxYear: Int): Unit = {
    val nokDebit = udf((date: String, baseCurrency: String) => {
      val input = Http(s"https://api.exchangeratesapi.io/$date?base=$baseCurrency&symbols=$currency").asString.body
      // println(input)

      val debit: Float = input.split("\"" + currency + "\":")(1).split('}')(0).toFloat
      // println(debit)

      debit
    })

    val dfInitialized: DataFrame = extracted
      .withColumn("Year",
        year(to_timestamp(col("Date and time (UTC)"), "dd/MM/yyyy HH:mm:ss")))
      .withColumn("Date",
        date_format(to_timestamp(col("Date and time (UTC)"), "dd/MM/yyyy HH:mm:ss"), "yyyy-MM-dd"))
      .withColumn("Exchange Rate",
        lit(nokDebit(col("Date"), col("Currency code"))))
      .withColumn("Debit (" + currency + ")", when(
        col("Debit") =!= "null",
        lit(col("Debit") * col("Exchange Rate"))))
      .withColumn("Credit (" + currency + ")", when(
        col("Credit") =!= "null",
        lit(col("Credit") * col("Exchange Rate"))))

    val dfTransactions: DataFrame = dfInitialized.where(col("Year") <= taxYear)
    val dfDividends: DataFrame = dfInitialized.where(col("Year") === taxYear)
    val properties: DataFrame = dfTransactions.select("Property Name").distinct()

    properties.collect().foreach { row =>
      val property = row.mkString

      if (!property.equals("null")) {
        // handle dividends
        val dfPropertyDividends: DataFrame = dfDividends.where(
          col("Property Name").contains(property)).where(
          col("Transaction Type").contains("Dividend"))
        val dividends = transformDividends(dfPropertyDividends, currency)

        // handle transactions
        val dfPropertyTransactions: DataFrame = dfTransactions.where(
          col("Property Name").contains(property)).where(
          col("Transaction Type").contains("Resale market"))
        val transactions = transformTransactions(dfPropertyTransactions, currency)

        load(property, dividends, transactions, currency)
      }
    }
  }


  private def transformDividends(df: DataFrame, currency: String): DataFrame = {
    val dividends: DataFrame = df
      .select(
        "Date and time (UTC)",
        "Transaction type",
        "Reference",
        "Property name",
        "Property symbol",
        "Credit",
        "Currency code",
        "Exchange Rate",
        "Credit (" + currency + ")"
      )
    dividends
  }


  private def transformTransactions(df: DataFrame, currency: String): DataFrame = {
    if (df.count() > 0) {
      var tradeCosts: DataFrame = df
        .groupBy("Date and time (UTC)")
        .pivot("Transaction type")
        .agg(first("Debit"))

      if (!tradeCosts.columns.contains("Resale market trade fees")) {
        tradeCosts = tradeCosts.withColumn(
          "Resale market trade fees",
          lit("null")
        )
      }

      if (!tradeCosts.columns.contains("Resale market trade tax")) {
        tradeCosts = tradeCosts.withColumn(
          "Resale market trade tax",
          lit("null")
        )
      }

      val transactions = df.as("df1").join(
        tradeCosts.as("df2"), df("Date and time (UTC)") === tradeCosts("Date and time (UTC)")
      ).select(
        "df1.Date and time (UTC)",
        "df1.Transaction type",
        "df1.Reference",
        "df1.Property name",
        "df1.Property symbol",
        "df1.Units",
        "df1.Avg Trade Price",
        "df1.Currency code",
        "df1.Debit",
        "df2.Resale market trade fees",
        "df2.Resale market trade tax",
        "df1.Exchange Rate",
        "df1.Debit (" + currency + ")")
        .filter(col("Units") =!= "null")
        .withColumnRenamed("Resale market trade fees", "Fees")
        .withColumnRenamed("Resale market trade tax", "Taxes")
        .withColumn("Fees (" + currency + ")",
          lit(col("Fees") * col("Exchange Rate"))
        ).withColumn("Taxes (" + currency + ")",
        lit(col("Taxes") * col("Exchange Rate"))
      )
      return transactions
    }
    null
  }


  def load(property: String, dividends: DataFrame, transactions: DataFrame, currency: String): Unit = {
    // dividend results
    println(s"$property - Dividends:")
    if (dividends != null && dividends.count() > 0) {
      printData("Dividends (" + currency + ")", dividends, "Credit (" + currency + ")")
      printData("Dividends (GBP)", dividends, "Credit")
      dividends.show(dividends.count().toInt, false)
    } else {
      println("No dividend data found.")
    }

    println()

    // transaction results
    println(s"$property - Transactions:")
    if (transactions != null && transactions.count() > 0) {
      printData("Debit (" + currency + ")", transactions, "Debit (" + currency + ")")
      printData("Fees  (" + currency + ")", transactions, "Fees (" + currency + ")")
      printData("Taxes (" + currency + ")", transactions, "Taxes (" + currency + ")")
      printData("Debit (GBP)", transactions, "Debit")
      printData("Fees  (GBP)", transactions, "Fees")
      printData("Taxes (GBP)", transactions, "Taxes")
      transactions.show(transactions.count().toInt, false)
    } else {
      println("No transaction data found.")
    }

    println()
    println()
  }

  private def printData(definition: String, df: DataFrame, column: String): Unit = {
    try {
      println(s"$definition: ${df.agg(sum(column)).first.getDouble(0).floor.toInt}")
    } catch {
      case _: Throwable => println(s"No value found for $definition.")
    }
  }
}
