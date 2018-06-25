//package gettingstarted

import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object WordCount {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
//    val outputFile = args(1)
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(_.split(" "))
    val counts = words.map(word=> (word, 1)).reduceByKey(_+_)
    counts.saveAsTextFile("/Users/naveenkumar/IdeaProjects/gettingstarted/src/main/resources/Testout.txt")

  }

}
