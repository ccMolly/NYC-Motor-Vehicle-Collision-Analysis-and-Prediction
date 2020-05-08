import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

// This file aims to change the schema of cleaned speed data from below

/*+-------------+--------------------+------------------+
|      BOROUGH|          DATA_AS_OF|        MEAN_SPEED|
+-------------+--------------------+------------------+
|        Bronx|08/12/2018 02:33:...| 50.73666666666666|
|        Bronx|08/12/2018 03:03:...|46.518750000000004|
|     Brooklyn|08/12/2018 04:13:...|             53.43|
|       Queens|08/12/2018 06:28:...| 50.59142857142857|
|Staten Island|08/12/2018 06:58:...|            44.425|
+-------------+--------------------+------------------+*/

// to this new schema

/*+-----+-------------+----------+------------------+                             
| TIME|      BOROUGH|      DATE|        MEAN_SPEED|
+-----+-------------+----------+------------------+
|12:38|        Bronx|08/13/2018|37.775999999999996|
| 5:18|     Brooklyn|08/13/2018| 48.30499999999999|
| 9:33|    Manhattan|08/27/2018|29.945999999999998|
| 4:53|Staten Island|10/14/2018| 30.36437500000001|
| 8:28|    Manhattan|11/18/2018|             31.06|
|20:42|    Manhattan|06/12/2019|             51.57|
+-----+-------------+----------+------------------+*/

// Basically, split the DATA_AS_OF to TIME and DATE
// Moreover, change AM/PM to 24 Hours type, e.g. 6PM --> 18:00
// This step is necessary for successful joining with other datasets


val sqlContext = new SQLContext(sc)

val processed_df = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/finalProject/processed_real_time_speed_2.csv","header" -> "true"))

// Split DATA_AS_OF column by ' ' into TIME and DATE
val speedData2 = processed_df.withColumn("splitcol",split(col("DATA_AS_OF")," ")).select(col("BOROUGH"),col("splitcol").getItem(0).as("DATE"),col("splitcol").getItem(1).as("Time"),col("splitcol").getItem(2).as("Meridiem"),col("MEAN_SPEED")).drop("splitcol")

val test = speedData2.rdd

// The following steps change PM/AM to 24 hours type: e.g. 6PM --> 18:00
val test2 = test.map(line => (line(3),line(2).toString.split(":").toList(0).toInt))

val test3 = test2.map(line => if (line._1=="PM"){(line._2+12).toString} else {line._2.toString})

val speedData3 = speedData2.withColumn("splitcol",split(col("Time"),":")).select(col("BOROUGH"),col("DATE"),col("splitcol").getItem(0).as("Hour"),col("splitcol").getItem(1).as("Minute"),col("MEAN_SPEED")).drop("splitcol")

val speedData4 = speedData3.withColumn("id",monotonicallyIncreasingId)

val test4 = test3.toDF

val test5 = test4.withColumn("id",monotonicallyIncreasingId)

val speedData5 = speedData4.join(test5,"id")

val speedData6 = speedData5.select(concat_ws(":",$"value",$"Minute") as "TIME",col("BOROUGH"),col("DATE"),col("MEAN_SPEED"))

speedData6.show(5)
		
// Already saved before, no need to overwrite
// speedData6.write.option("header", "true").format("csv").save("data/speed_data_new")
		
		
