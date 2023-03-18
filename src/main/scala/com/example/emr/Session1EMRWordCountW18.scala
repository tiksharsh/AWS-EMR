package com.example.emr

import org.apache.spark.SparkContext
import java.util.logging.{Level, Logger}

object Session1EMRWordCountW18 extends App {
  Logger.getLogger("org").setLevel(Level.ALL)


//  ############# This is for local file read on your system #############
/*
  val sc = new SparkContext("local[*]", "word-count")
  val input = sc.textFile("/Users/Wolverine/Documents/BigData-Hadoop/Week 9/DataSets/search_data.txt")
 */

//   ############# This is for file read on your AMAZON S3 Bucket #############

  val sc = new SparkContext()
  val input = sc.textFile("s3://trendytech-harsh/search_data.txt")

  val words = input.flatMap(x => x.split(" "))


  val wordsLower = words.map(x => x.toLowerCase())
  val wordCount = wordsLower.map( x => (x,1))

  val finalCount = wordCount.reduceByKey((a,b) => a+b)
  val reversedTuple = finalCount.map(x => (x._2,x._1))
  val sortedResults = reversedTuple.sortByKey(false).map(x => (x._2,x._1))
  sortedResults.collect.foreach(println)
//  val results = sortedResults.collect()
//
//  for(result <- results) {
//    val word = result._1
//    val count = result._2
//    println(s"$word : $count")
//  }
  println {
    "harsh"
  }
  scala.io.StdIn.readLine()
  //aws s3 cp s3://bucket-name/file_name.jar .
  //spark-submit --class classname file_name.jar
}
