import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Profile {

	def main(args: Array[String]) {
		val sc = new SparkContext()
		val sqlContext = new SQLContext(sc)
		val origi_df = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/finalProject/DOT_Traffic_Speeds_NBE_2.csv","header" -> "true"))
		val processed_df = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/finalProject/processed_real_time_speed_2.csv","header" -> "true"))
		origi_df.show(10,true)
		processed_df.show(10,true)

		// Long = 35763997
		origi_df.count()

		// Long = 5222755
		processed_df.count()

		val origi_group = origi_df.groupBy("BOROUGH").count()
		origi_group.show(true)

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

		val processed_group = processed_df.groupBy("BOROUGH").count()
		processed_group.show(true)

		/*	+-------------+-------+
			|      BOROUGH|  count|
			+-------------+-------+
			|       Queens|1387647|
			|     Brooklyn| 774447|
			|Staten Island| 794372|
			|    Manhattan|1218543|
			|        Bronx|1047746|
			+-------------+-------+
		*/

		val speed = processed_df.agg(min("MEAN_SPEED"), max("MEAN_SPEED"))
		speed.show()

		/*
			+---------------+---------------+
			|min(MEAN_SPEED)|max(MEAN_SPEED)|
			+---------------+---------------+
			|           1.24|          98.79|
			+---------------+---------------+
		*/
		sc.stop()
	}
}
