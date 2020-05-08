import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.SparkConf 
import scala.collection.mutable.ArrayBuffer 
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext._ 
import sqlContext.implicits._

// read the cleaned cvs as dataframe
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("proj/data.csv")
// val origindf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("proj/Motor_Vehicle_Collisions_-_Crashes.csv")

df.dtypes.foreach(println)


// res15: Long = 1671388
// origindf.count()

// count the dataframe, res15: Long = 1671388
df.count()





