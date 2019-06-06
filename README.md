# Scala-For-Biggeners
This Repo helps to understand from the Scala Basics to advance level.

The Question was asked in one of my interviews, that there are two tables 1.customerdimension 2.customer
customerdimension contains all the records regarding customer i.e. id,name,age,address...etc - 1L records are there
cust file contains only 2 columns id,address, so in both tables having 'custid' common column, and using that 
we have to update the address column in customerdimension.

package com.scala.basics

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions._


object customerUpateDetails {

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
    
    val custDimensionRdd = custDimension.filter(x => x != header)
                                        .map(x => x.split("\\s+"))
                                        .map(x => (x(0).toInt,x(1).toString.trim,x(2).toString.trim))
                                        .toDF("eid","ename","addr")
                                        
    val custrdd = sc.textFile("C:\\spark\\warehouse\\cust.txt", 2)
                    .map(x => x.split("\\s+"))
                    .map(x => (x(0).toInt,x(1).toString().trim()))
                    .toDF("cid","caddr")
                    
    
      /*val updatedValues =  custDimensionRdd.as("a").join(custrdd.as("b"), ($"a.eid" === $"b.cid"))
                                .select($"a.eid", $"a.ename", $"b.caddr")
      */
       custDimensionRdd.createOrReplaceTempView("custd");
       custrdd.createOrReplaceTempView("cust");
       
       val finalresult = spark.sql("""select a.eid as id, a.ename, 
         case when eid = cid then caddr else addr end as addr from custd a 
         left outer join cust b on eid==cid order by id""")
         
         finalresult.show()
                                  
                                                     

  }

}
