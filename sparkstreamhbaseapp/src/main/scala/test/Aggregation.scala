package test

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel
import java.io._
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
import java.io.{ FileInputStream, InputStream, PrintStream, File => JFile }
import au.com.bytecode.opencsv.CSVParser

import scala.io.Source
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object Aggregation {
  
   val sparkConf = new SparkConf().setAppName("Aggregation")
   val sparkContext = new SparkContext()
   
  def main(args: Array[String]) {
    
    if(args.length < 1){
      println("USAGE : <outputDirectory>")
      System.exit(0)
    }
   
    val outputDirectory = args(0)
    
    //Read Files from Output Directory
    val rdd1 = readFilesFromDirectory(outputDirectory) 
   
    if(rdd1.length > 0)
      aggregateData(rdd1,outputDirectory)
    else
      System.exit(0)
    
  }
  
  def readFilesFromDirectory(outputDirectory:String) :  Array[(String, String)] ={
    val readFiles = sparkContext.wholeTextFiles(outputDirectory, 100)
    println(readFiles.collect())
    val rdd1 = readFiles.collect()
    println(rdd1.length)
    rdd1
  }
 
  def aggregateData(rdd1:Array[(String,String)],outputDirectory:String) : Boolean={
     println(rdd1.grouped(2))
     for(i <- 0 until rdd1.length){
       // println("i is: " + i);
       // println("i'th element is: " + rdd1(i));
        val data = rdd1(i);
        val fileName = data._1 //FileName
        
        // val bufferedSource = Source.fromFile(fileName)  
        val content = data._2 //Content
        
        content.map(line => {
         val parser = new CSVParser(',')
        parser.parseLine(content).mkString(",")
         //parser.parseLine(line).mkString(",")
       }).take(5).foreach(println)
//         for (line <- content.split("\n")) {
//           println("+++++++++++++++")
//          // println(line)
//          val cols = line.split(",").map { x => x.groupBy { cols(0) => ??? } }
//        }
        //println(content)
        
        //startAggregation(content)
     }
     true
  }
  
  def startAggregation(content:String) {
    
    while(content != null){
      val splittedData = content.split(",")
      for(i <- 0 until splittedData.length ) {
        val key = splittedData(i);
        println(key)
        //val value = splittedData(i);
      }
    }
    val splittedData = content.split(",")//.collect { case Array(k, v) => (k, v) }.toMap
    //println(splittedData.splitAt(n))
    //Some(splittedData(0), splittedData(1))
   // val value = splittedData.groupBy { x => ??? }
//    val value = splittedData.groupBy { x => x.contains("/") }
//     println(value.values)
//     val test = value.values
//     for(i<-0 until test.size)
       
    
  }
  
}