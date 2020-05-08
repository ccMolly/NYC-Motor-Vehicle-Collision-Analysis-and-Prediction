import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Clean {

	def main(args: Array[String]) {
		val sc = new SparkContext()
		val sqlContext = new SQLContext(sc)
		val df = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/finalProject/DOT_Traffic_Speeds_NBE_2.csv","header" -> "true"))
		df.show(10,true)
		// change Staten island to Staten Island
		val updatedDF = df.withColumn("BOROUGH", regexp_replace(df("BOROUGH"), "i", "I"))
		val test = updatedDF.groupBy("BOROUGH").count()
		test.show(true)
		// from

		/*  +-------------+--------+                                                        
			|      BOROUGH|   count|
			+-------------+--------+
			|       Queens|10381975|
			|     Brooklyn| 4314488|
			|Staten Island| 7858145|
			|Staten island|  265323|
			|    Manhattan| 6861340|
			|        Bronx| 6082726|
			+-------------+--------+ */

		//to

		/*  +-------------+--------+                                                        
			|      BOROUGH|   count|
			+-------------+--------+
			|       Queens|10381975|
			|     Brooklyn| 4314488|
			|Staten Island| 8123468|
			|    Manhattan| 6861340|
			|        Bronx| 6082726|
			+-------------+--------+  */

		// delete row with 0 speed or 0 travel time
		val newDF = updatedDF.filter(updatedDF("SPEED")>0 && updatedDF("TRAVEL_TIME")>0)
		// only leave column SPEED, DATA_AS_OF and BOROUGH
		val newDF_2 = newDF.drop("ID","TRAVEL_TIME","STATUS","LINK_ID","LINK_POINTS","ENCODED_POLY_LINE","ENCODED_POLY_LINE_LVLS","OWNER","TRANSCOM_ID","LINK_NAME")
		// delete row with time early than 2017.1.1
		val newDF_3 = newDF_2.filter(newDF_2("DATA_AS_OF").contains("/2017")||newDF_2("DATA_AS_OF").contains("/2018")||newDF_2("DATA_AS_OF").contains("/2019")||newDF_2("DATA_AS_OF").contains("/2020"))
		// group by time and borough calculate mean speed
		val newDF_4 = newDF_3.groupBy("BOROUGH", "DATA_AS_OF").agg(mean("SPEED") as "MEAN_SPEED")
		newDF_4.write.option("header", "true").format("csv").save("finalProject/processed_real_time_speed_2.csv")
		sc.stop()
	}
}
