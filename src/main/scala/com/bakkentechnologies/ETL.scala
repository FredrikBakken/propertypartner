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


  def transform(extracted: DataFrame, taxYear: Int): Unit = {
    val nokDebit = udf((date: String, currencyCode: String) => {
      val input = Http(s"https://api.exchangeratesapi.io/$date?base=$currencyCode&symbols=NOK").asString.body
      // println(input)

      val debit: Float = input.split("\"NOK\":")(1).split('}')(0).toFloat
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
      .withColumn("Debit (NOK)", when(
        col("Debit") =!= "null",
        lit(col("Debit") * col("Exchange Rate"))))
      .withColumn("Credit (NOK)", when(
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
        val dividends = transformDividends(dfPropertyDividends)

        // handle transactions
        val dfPropertyTransactions: DataFrame = dfTransactions.where(
          col("Property Name").contains(property)).where(
          col("Transaction Type").contains("Resale market"))
        val transactions = transformTransactions(dfPropertyTransactions)

        load(property, dividends, transactions)
      }
    }
  }


  private def transformDividends(df: DataFrame): DataFrame = {
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
        "Credit (NOK)"
      )
    dividends
  }


  private def transformTransactions(df: DataFrame): DataFrame = {
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
        "df1.Debit (NOK)")
        .filter(col("Units") =!= "null")
        .withColumnRenamed("Resale market trade fees", "Fees")
        .withColumnRenamed("Resale market trade tax", "Taxes")
        .withColumn("Fees (NOK)",
          lit(col("Fees") * col("Exchange Rate"))
        ).withColumn("Taxes (NOK)",
        lit(col("Taxes") * col("Exchange Rate"))
      )
      return transactions
    }
    null
  }


  def load(property: String, dividends: DataFrame, transactions: DataFrame): Unit = {
    // dividend results
    println(s"$property - Dividends:")
    if (dividends != null && dividends.count() > 0) {
      printData("Dividends (NOK)", dividends, "Credit (NOK)", "", " kr")
      printData("Dividends (GBP)", dividends, "Credit", "£ ", "")
      dividends.show(dividends.count().toInt, false)
    } else {
      println("No dividend data found.")
    }

    println()

    // transaction results
    println(s"$property - Transactions:")
    if (transactions != null && transactions.count() > 0) {
      printData("Debit (NOK)", transactions, "Debit (NOK)", "", " kr")
      printData("Fees  (NOK)", transactions, "Fees (NOK)", "", " kr")
      printData("Taxes (NOK)", transactions, "Taxes (NOK)", "", " kr")
      printData("Debit (GBP)", transactions, "Debit", "£ ", "")
      printData("Fees  (GBP)", transactions, "Fees", "£ ", "")
      printData("Taxes (GBP)", transactions, "Taxes", "£ ", "")
      transactions.show(transactions.count().toInt, false)
    } else {
      println("No transaction data found.")
    }

    println()
    println()
  }

  private def printData(definition: String, df: DataFrame, column: String, dollar: String, kroner: String): Unit = {
    try {
      println(s"$definition: $dollar${df.agg(sum(column)).first.getDouble(0).floor.toInt}$kroner")
    } catch {
      case _: Throwable => println(s"No value found for $definition.")
    }
  }
}
