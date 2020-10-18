package com.FileLoad
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when,_}

object FileLoad  {
	def main(args:Array[String]): Unit = {
    	   val spark: SparkSession = SparkSession.builder()
      		.master("local[1]")
      		.appName("FileLoad")
      		.getOrCreate()
           val sc = spark.sparkContext

      val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("/src/main/auth.csv")

      val df1 = df.filter(df("aua")=!="650000")
     
      val df2 = df1.withColumn("sa", when(col("sa")==="SERVICEMON",col("aua").otherwise(col("sa"))))

      val df3 = df2.filter(df2("aua")===df2("asa")

      df3.write.mode(SaveMode.Append).json("/tgt/main/auth.json")
}
