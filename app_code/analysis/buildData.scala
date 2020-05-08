import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object BuildData {
	def main(args : Array[String]) : Unit = {
		val sc = new SparkContext()
		// val sqlContext = new SQLContext(sc)
		val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		// readin three datasets
		val collisiondata = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/data/data-collision-new/","header" -> "true"))
		val weatherData = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/data/weather_etl_new/","header" -> "true"))
		val speedData = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/data/speed_data_new/","header" -> "true"))
		val collisionData = collisiondata.withColumnRenamed("BOROUGH","BOROUGH_1")
							.withColumnRenamed("CRASH DATE","CRASH_DATE")
							.withColumnRenamed("CRASH TIME","CRASH_TIME")
		// Join 3 datasets
		val trainData = collisionData.join(speedData,speedData("DATE")===collisionData("CRASH_DATE") && speedData("TIME")===collisionData("CRASH_TIME")&&speedData("BOROUGH")===collisionData("BOROUGH_1"))
		val traindata = trainData.drop("DATE","TIME","BOROUGH")
		val trainData_1 = traindata.join(weatherData,weatherData("DATE")===traindata("CRASH_DATE") && weatherData("BOROUGH")===traindata("BOROUGH_1"))
		val train_new = trainData_1.filter(trainData_1("NUMBER OF PERSONS INJURED").isNotNull &&trainData_1("NUMBER OF PERSONS KILLED").isNotNull)
		val trainData_2 = train_new.rdd.map(a => Vectors.dense(a(9).toString.toDouble, a(10).toString.toDouble))
		val model = KMeans.train(trainData_2,3,1)
		val err = model.computeCost(trainData_2)
		val result = model.predict(trainData_2)
		val result_df = result.toDF.withColumn("id",monotonicallyIncreasingId)
		val train = train_new.withColumn("id",monotonicallyIncreasingId)
		val train_with_risk = train.join(result_df,"id")
		val train_with_risk_level = train_with_risk.withColumnRenamed("value","RISK_LEVEL").drop("id","BOROUGH","DATE")

		// Aready exist no need to write again
		// train_with_risk_level.write.option("header", "true").format("csv").save("data/train_with_risk_level_new")
		
		val train_with_risk_level_new = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/data/train_with_risk_level_new/","header" -> "true"))

		// Split CRASH_DATE and CRASH_TIME into YEAR MONTH and HOUR features for further analysis
		val test = train_with_risk_level_new.withColumn("MONTH",split(col("CRASH_DATE"),"/").getItem(0).cast("Int"))
					.withColumn("YEAR",split(col("CRASH_DATE"),"/").getItem(2))
					.withColumn("HOUR",split(col("CRASH_TIME"),":").getItem(0).cast("Int"))

		// Rename column, chang space to '_' (otherwise Tableau will come up with error)
		val a = test.withColumnRenamed("ZIP CODE","ZIP_CODE")
				.withColumnRenamed("ON STREET NAME","ON_STREET_NAME")
				.withColumnRenamed("CROSS STREET NAME","CROSS_STREET_NAME")
				.withColumnRenamed("NUMBER OF PERSONS INJURED","NUMBER_OF_PERSONS_INJURED")
				.withColumnRenamed("NUMBER OF PERSONS KILLED","NUMBER_OF_PERSONS_KILLED")

		// Reorder cluster labels to let them represent increasing risk level
		val df = a.withColumn("RISK_LEVEL",a("RISK_LEVEL").cast("String"))

		val updatedDF = df.withColumn("RISK_LEVEL", regexp_replace(df("RISK_LEVEL"), "1", "3"))

		val updatedDF2 = updatedDF.withColumn("RISK_LEVEL", regexp_replace(updatedDF("RISK_LEVEL"), "2", "1"))

		val updatedDF3 = updatedDF2.withColumn("RISK_LEVEL", regexp_replace(updatedDF2("RISK_LEVEL"), "3", "2"))

		val b = updatedDF3.withColumn("RISK_LEVEL",updatedDF3("RISK_LEVEL").cast("Int"))
		
		// Already exist no need to write again		
		//b.write.saveAsTable("mc7805.trainDataVer5")

		// Already exist no need to write again	
		//b.write.option("header", "true").format("csv").save("data/train_data_latest_version_4")


		sc.stop()
	}
}
