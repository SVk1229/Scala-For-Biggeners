package com.scala.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import scala.tools.jline_embedded.internal.Nullable
import org.apache.spark.sql.types.StringType

object interViewQ {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("customerDimension")
      .master("local[*]")
      .config("spark.metastore.warehouse.dir", "C:/spark/warehouse")
      .getOrCreate()

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*val custDimFile = scala.io
                      .Source
                      .fromFile("C:/spark/warehouse/custdimension (3).csv")
                      .getLines()
                      .toList*/

    // To convert RDD to DF/DS
    import spark.implicits._

    val sc = spark.sparkContext

    val custDimension = sc.textFile("C:\\spark\\warehouse\\custdimension.txt", 2)

    val header = custDimension.first()

    //Programatically Specified Schema Using Row
    
    val custDimensionRdd = custDimension.filter(x => x != header)
      .map(x => x.split("\\s+"))
      .map(x => Row(x(0).toInt, x(1).toString.trim, x(2).toString.trim))  //need to import Row Library
     // .toDF("eid", "ename", "addr")
      
     val custSchema = StructType(Array(StructField("eid",IntegerType,true),
                                       StructField("ename",StringType,true),
                                       StructField("addr",StringType,true)))
                                       
      val custDDf = spark.createDataFrame(custDimensionRdd, custSchema)

    // Manually Specifying schema
      
    val custrdd = sc.textFile("C:\\spark\\warehouse\\cust.txt", 2)
      .map(x => x.split("\\s+"))
      .map(x => (x(0).toInt, x(1).toString().trim()))
      .toDF("cid", "caddr")

    custDDf.createOrReplaceTempView("custd");
    custrdd.createOrReplaceTempView("cust");

    val finalresult = spark.sql("""select a.eid as id, a.ename, 
         case when eid = cid then caddr else addr end as addr from custd a 
         left outer join cust b on eid==cid order by id""")

    finalresult.show()
/*    
+---+---------+----+
| id|    ename|addr|
+---+---------+----+
|  1|      sai| BLR|
|  2|  venkata| BLR|
|  3|  krishna| BLR|
|  4|   prasad| BLR|
|  5|     rama| BLR|
|  6|     arun| hyd|
|  7|prashanth| hyd|
|  8|    sunny| hyd|
|  9|  shwetha| hyd|
| 10|    suved| hyd|
| 11|   chinnu| hyd|
| 12|    bittu| hyd|
| 13|    kittu| hyd|
| 14|    pawan| hyd|
| 15|   kalyan| hyd|
| 16|     anni| hyd|
| 17|    marie| hyd|
| 18| shreyasi| hyd|
| 19|    ghosh| hyd|
| 20|     simi| hyd|
+---+---------+----+*/



  }

}
