import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.SparkConf 
import scala.collection.mutable.ArrayBuffer 
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext._ 
import sqlContext.implicits._

// read csv file as dataframe
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("proj/Motor_Vehicle_Collisions_-_Crashes.csv")
// show the schema of dataframe
df.schema

df.show(1)
// print the datatype of each column
df.dtypes.foreach(println)
// count the number of rows in dataframe
df.count()
// select the columns we need
val selectedDF = df.select(df("COLLISION_ID"), df("CRASH DATE"), df("CRASH TIME"), df("BOROUGH"), df("ZIP CODE"), df("LATITUDE"), df("LONGITUDE"), df("ON STREET NAME"), df("CROSS STREET NAME"), df("NUMBER OF PERSONS INJURED"), df("NUMBER OF PERSONS KILLED") )

selectedDF.dtypes.foreach(println)
// save the new dataframe
selectedDF.write.format("com.databricks.spark.csv").option("header", "true").save("proj/data.csv")
