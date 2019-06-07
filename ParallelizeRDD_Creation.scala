package com.scala.basics

import org.apache.spark.sql._
import org.apache.log4j._

object Transformations {
  
  def main(args : Array[String]): Unit = {
    
   
    val spark = SparkSession.builder()
                            .appName("ParalellizeRDD")
                            .config("spark.metastore.warehouse.dir", "C:/spark/warehouse")
                            .master("local[*]")
                            .enableHiveSupport()
                            .getOrCreate()
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val parallelizeRdd1 = spark.sparkContext.parallelize(Seq(("sai",23),("venkat",24)))
    var parallelizeRdd2 = spark.sparkContext.parallelize((1 to 10), 2)    
    val parallelizeRdd3 = spark.sparkContext.parallelize(List(1 to 10), 2)
    val parallelizeRdd4 = spark.sparkContext.parallelize(List(("sai",23),("venkat",24)))
    
    val parallelizeRddFile = scala.io.Source.fromFile("C:\\spark\\warehouse\\custdimension.txt")
                                         .getLines()
                                         .toList
    val parallelizeRdd5 = spark.sparkContext.parallelize(parallelizeRddFile, 2) 
    
    // parallelizeRdd1 Output 
    parallelizeRdd1.take(10).foreach(println)
    
    /*
     * (sai,23)
    	 (venkat,24)
    */
    
    //parallelizeRdd2
    parallelizeRdd2.take(10).foreach(println)
  
  /*  
    1
		2
    3
    4
    5
    6
    7
    8
    9
    10
 */
    
    
    // parallelizeRdd3 Output 
    parallelizeRdd3.take(10).foreach(println)
    
 //   Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    
    //parallelizeRdd4
    parallelizeRdd4.take(10).foreach(println)
    
   /* (sai,23)
    (venkat,24)*/
    
    // parallelizeRdd5 Output 
    parallelizeRdd5.take(10).foreach(println)
    
/*   id	name	address
      1	sai	hyd
      2	venkata	hyd
      3	krishna	hyd
      4	prasad	hyd
      5	rama	hyd
      6	arun	hyd
      7	prashanth	hyd
      8	sunny	hyd
      9	shwetha	hyd
      
 */
    
    
  }
}
