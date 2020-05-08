import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Model {
	def main(args : Array[String]) : Unit = {
		val sc = new SparkContext()
		val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._

		val train_data = sqlContext.load("com.databricks.spark.csv", Map("path" ->"hdfs:///user/mc7805/data/train_data_latest_version_4/","header" -> "true"))

		// Change BOROUGH_1 from string to unique integer label: Brooklyn=0 Queens=1 Staten Island=2

		val updatedDF = train_data.withColumn("BOROUGH_1", regexp_replace(train_data("BOROUGH_1"), "Brooklyn", "0"))

		val updatedDF2 = updatedDF.withColumn("BOROUGH_1", regexp_replace(updatedDF("BOROUGH_1"), "Queens", "1"))

		val updatedDF3 = updatedDF2.withColumn("BOROUGH_1", regexp_replace(updatedDF2("BOROUGH_1"), "Staten Island", "2"))

		val updatedDF4 = updatedDF3.withColumn("BOROUGH_1", regexp_replace(updatedDF3("BOROUGH_1"), "Bronx", "3"))

		val trainData = updatedDF4.withColumn("BOROUGH_1", regexp_replace(updatedDF4("BOROUGH_1"), "Manhattan", "4"))

		// Change All feature columns' type to float (otherwise model training will come up with error)

		val df = trainData.withColumn("BOROUGH_1",trainData("BOROUGH_1").cast("Float"))

		val a = df.withColumn("SNOW",df("SNOW").cast("Float")).withColumn("NUMBER_OF_PERSONS_INJURED",df("NUMBER_OF_PERSONS_INJURED").cast("Float")).withColumn("NUMBER_OF_PERSONS_KILLED",df("NUMBER_OF_PERSONS_KILLED").cast("Float")).withColumn("AWND",df("AWND").cast("Float")).withColumn("PRCP",df("PRCP").cast("Float")).withColumn("SNWD",df("SNWD").cast("Float")).withColumn("TAVG",df("TAVG").cast("Float")).withColumn("TMAX",df("TMAX").cast("Float")).withColumn("TMIN",df("TMIN").cast("Float")).withColumn("WDF2",df("WDF2").cast("Float")).withColumn("WDF5",df("WDF5").cast("Float")).withColumn("WSF2",df("WSF2").cast("Float")).withColumn("RISK_LEVEL",df("RISK_LEVEL").cast("Float")).withColumn("MONTH",df("MONTH").cast("Float")).withColumn("YEAR",df("YEAR").cast("Float")).withColumn("HOUR",df("HOUR").cast("Float")).withColumn("MEAN_SPEED",df("MEAN_SPEED").cast("Float"))

		// Assemble these 9 features to one new column 'features'

		val assembler = new VectorAssembler().setInputCols(Array("BOROUGH_1", "MEAN_SPEED", "MONTH","HOUR","AWND","PRCP","SNOW","SNWD","TAVG")).setOutputCol("features")

		val df2 = assembler.transform(a)

		// Transform "RISK_LEVEL" column to "label" column

		val labelIndexer2 = new StringIndexer().setInputCol("RISK_LEVEL").setOutputCol("label")

		val df3 = labelIndexer2.fit(df2).transform(df2)

		// Statistic of NUMBER OF PERSONS INJURED

		df3.agg(max(df3(df3.columns(9)))).show

		df3.agg(min(df3(df3.columns(9)))).show

		df3.agg(mean(df3(df3.columns(9)))).show

		/*+------------------------------+-----------------------------+------------------------------+
		|min(NUMBER OF PERSONS INJURED)|max(NUMBER OF PERSONS INJURED)|avg(NUMBER OF PERSONS INJURED)|
		+------------------------------+-----------------------------+------------------------------+
		|                           0.0|                          11.0|            0.2993677400382297|
		+------------------------------+-----------------------------+------------------------------+*/

		// Statistic of NUMBER OF PERSONS KILLED

		df3.agg(max(df3(df3.columns(10)))).show

		df3.agg(min(df3(df3.columns(10)))).show

		df3.agg(mean(df3(df3.columns(10)))).show

		/*+-----------------------------+-----------------------------+-----------------------------+
		|min(NUMBER OF PERSONS KILLED)|max(NUMBER OF PERSONS KILLED)|avg(NUMBER OF PERSONS KILLED)|
		+-----------------------------+-----------------------------+-----------------------------+
		|                          0.0|                          1.0|         0.002095280105866...|
		+-----------------------------+-----------------------------+-----------------------------+*/

		// Statistic of MEAN_SPEED

		df3.agg(min(df3(df3.columns(11)))).show

		df3.agg(min(df3(df3.columns(11)))).show

		df3.agg(mean(df3(df3.columns(11)))).show

		df3.agg(stddev(df3(df3.columns(11)))).show

		/*+---------------+---------------+-----------------+-----------------------+     
		|min(MEAN_SPEED)|max(MEAN_SPEED)|  avg(MEAN_SPEED)|stddev_samp(MEAN_SPEED)|
		+---------------+---------------+-----------------+-----------------------+
		|           1.24|         101.28|37.87083107961782|     10.890241584206779|
		+---------------+---------------+-----------------+-----------------------+*/

		// Statistic of AWND

		df3.agg(min(df3(df3.columns(12)))).show

		df3.agg(min(df3(df3.columns(12)))).show

		df3.agg(mean(df3(df3.columns(12)))).show

		df3.agg(stddev(df3(df3.columns(12)))).show

		/*+---------+---------+------------------+------------------+                     
		|min(AWND)|max(AWND)|         avg(AWND)| stddev_samp(AWND)|
		+---------+---------+------------------+------------------+
		|     1.34|    28.19|10.447593179612873|3.8772613113615844|
		+---------+---------+------------------+------------------+*/

		// Statistic of PRCP

		df3.agg(min(df3(df3.columns(13)))).show

		df3.agg(min(df3(df3.columns(13)))).show

		df3.agg(mean(df3(df3.columns(13)))).show

		df3.agg(stddev(df3(df3.columns(13)))).show

		/*+---------+---------+------------------+-------------------+                    
		|min(PRCP)|max(PRCP)|         avg(PRCP)|  stddev_samp(PRCP)|
		+---------+---------+------------------+-------------------+
		|      0.0|   2.5375|0.1529990646950737|0.30891726481759585|
		+---------+---------+------------------+-------------------+*/

		// Statistic of SNOW

		df3.agg(min(df3(df3.columns(14)))).show

		df3.agg(min(df3(df3.columns(14)))).show

		df3.agg(mean(df3(df3.columns(14)))).show

		df3.agg(stddev(df3(df3.columns(14)))).show

		/*+---------+---------+-------------------+------------------+                    
		|min(SNOW)|max(SNOW)|          avg(SNOW)| stddev_samp(SNOW)|
		+---------+---------+-------------------+------------------+
		|      0.0|     9.95|0.06720258956478425|0.5251802075151446|
		+---------+---------+-------------------+------------------+*/

		// Statistic of SNWD

		df3.agg(min(df3(df3.columns(15)))).show

		df3.agg(min(df3(df3.columns(15)))).show

		df3.agg(mean(df3(df3.columns(15)))).show

		df3.agg(stddev(df3(df3.columns(15)))).show

		/*+---------+---------+-------------------+------------------+                    
		|min(SNWD)|max(SNWD)|          avg(SNWD)| stddev_samp(SNWD)|
		+---------+---------+-------------------+------------------+
		|      0.0|     49.5|0.13413288282346758|1.4137508660675624|
		+---------+---------+-------------------+------------------+*/

		// Statistic of TAVG

		df3.agg(min(df3(df3.columns(16)))).show

		df3.agg(min(df3(df3.columns(16)))).show

		df3.agg(mean(df3(df3.columns(16)))).show

		df3.agg(stddev(df3(df3.columns(16)))).show

		/*+---------+---------+------------------+-----------------+                      
		|min(TAVG)|max(TAVG)|         avg(TAVG)|stddev_samp(TAVG)|
		+---------+---------+------------------+-----------------+
		|      9.0|     92.0|54.825797676812236|17.21868402561499|
		+---------+---------+------------------+-----------------+*/

		// Statistic of TMAX

		df3.agg(min(df3(df3.columns(17)))).show

		df3.agg(min(df3(df3.columns(17)))).show

		df3.agg(mean(df3(df3.columns(17)))).show

		df3.agg(stddev(df3(df3.columns(17)))).show

		/*+---------+---------+------------------+------------------+                     
		|min(TMAX)|max(TMAX)|         avg(TMAX)| stddev_samp(TMAX)|
		+---------+---------+------------------+------------------+
		|     13.5|    101.0|61.843874062637845|18.101948208505803|
		+---------+---------+------------------+------------------+*/

		// Statistic of TMIN

		df3.agg(min(df3(df3.columns(18)))).show

		df3.agg(min(df3(df3.columns(18)))).show

		df3.agg(mean(df3(df3.columns(18)))).show

		df3.agg(stddev(df3(df3.columns(18)))).show

		/*+---------+---------+-----------------+-----------------+                       
		|min(TMIN)|max(TMIN)|        avg(TMIN)|stddev_samp(TMIN)|
		+---------+---------+-----------------+-----------------+
		|      2.5|     84.0|47.77090685193354|17.04501042428105|
		+---------+---------+-----------------+-----------------+*/


		// Index labels, adding metadata to the label column

		// Fit on whole dataset to include all labels in index

		val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df3)

		// Automatically identify categorical features, and index them

		val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(25).fit(df3)

		// Set random seed for re-implementation

		val splitSeed = 5043

		// Randomly split dataset into training data(70%) and test data(30%)

		val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)

		// ============ LogisticRegression ==============================================

		val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

		// Train model
		val lrModel = lr.fit(trainingData)

		// Details of trained model
		val trainingSummary = lrModel.summary

		trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) => println(s"label $label: $rate")}

		trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) => println(s"label $label: $rate")}

		trainingSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>println(s"label $label: $prec")}

		// ============ End of LogisticRegression ==============================================

		// ============ RandomForestClassifier ============================================

		val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(5)

		// Convert indexed labels back to original labels
		val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

		// Chain indexers and tree in a Pipeline
		val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

		// Train model
		val model = pipeline.fit(trainingData)

		// Make predictions
		val predictions = model.transform(testData)

        // Select (prediction, true label) and compute test error
		val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

		val accuracy = evaluator.evaluate(predictions)

		println(s"Test Acc = ${(accuracy)}") // 0.7906072491209089

		// Show whole structure of trained random forest classfier
		val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]

		println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

		// Get feature importance list
		val importance = rfModel.featureImportances

		val featureCols = Array("BOROUGH_1", "MEAN_SPEED", "MONTH","HOUR","AWND","PRCP","SNOW","SNWD","TAVG")

		// Combine importance with corresponding feature and sort
		val res = featureCols.zip(importance.toArray).sortBy(-_._2)

		res.foreach(println)

		/*  ---- FEATURE IMPORTANCES -----

			(HOUR,0.504728092813846)
			(BOROUGH_1,0.15864827118984173)
			(MONTH,0.10190038417330667)
			(AWND,0.08875376613095234)
			(TAVG,0.05409326026080956)
			(PRCP,0.031945158488081216)
			(MEAN_SPEED,0.028006987588438087)
			(SNOW,0.016802139617267325)
			(SNWD,0.015121939737457141)  */

		// ============ End of RandomForestClassifier ============================================


		// ============ DecisionTreeClassifier ============================================

		val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxDepth(8)

        // Convert indexed labels back to original labels
		val labelConverter2 = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

		// Chain indexers and tree in a Pipeline
		val pipeline2 = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter2))

		// Train model
		val model2 = pipeline2.fit(trainingData)

		// Save trained parameters
		//model2.write.overwrite().save("data/treeModelNew2")

		// Make predictions
		val predictions2 = model2.transform(testData)

		// Select (prediction, true label) and compute test error
		val evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

		val accuracy2 = evaluator2.evaluate(predictions2)

		println(s"Test Acc = ${(accuracy2)}") //0.7905058155261022

		// Show whole structure of trained decision tree classfier
		val treeModel = model2.stages(2).asInstanceOf[DecisionTreeClassificationModel]

		println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

		// Test loading model
		val sameModel = PipelineModel.load("data/treeModelNew2")

		// ============ End of DecisionTreeClassifier ============================================

		// ============ End of Model Training and Selection ============================================


		// Calculate correlation between each 2 features
		// All the results are very low which means multicollinearity is proved to be weak
		a.stat.corr("RISK_LEVEL", "SNOW")
		a.stat.corr("RISK_LEVEL", "AWND")
		a.stat.corr("RISK_LEVEL", "PRCP")
		a.stat.corr("RISK_LEVEL", "SNWD")
		a.stat.corr("RISK_LEVEL", "TAVG")
		a.stat.corr("RISK_LEVEL", "WDF2")
		a.stat.corr("RISK_LEVEL", "WSF2")
		a.stat.corr("RISK_LEVEL", "MONTH")
		a.stat.corr("RISK_LEVEL", "YEAR")
		a.stat.corr("RISK_LEVEL", "HOUR")
		a.stat.corr("RISK_LEVEL", "MEAN_SPEED")
		a.stat.corr("RISK_LEVEL", "NUMBER_OF_PERSONS_INJURED")
		a.stat.corr("RISK_LEVEL", "NUMBER_OF_PERSONS_KILLED")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "MEAN_SPEED")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "SNOW")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "SNWD")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "AWND")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "PRCP")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "TAVG")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "WDF2")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "WSF2")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "MONTH")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "YEAR")
		a.stat.corr("NUMBER_OF_PERSONS_INJURED", "HOUR")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "MEAN_SPEED")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "SNOW")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "SNWD")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "AWND")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "PRCP")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "TAVG")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "WDF2")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "WSF2")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "MONTH")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "YEAR")
		a.stat.corr("NUMBER_OF_PERSONS_KILLED", "HOUR")
		a.stat.corr("SNOW", "MEAN_SPEED")
		a.stat.corr("SNWD", "MEAN_SPEED")
		a.stat.corr("AWND", "MEAN_SPEED")
		a.stat.corr("PRCP", "MEAN_SPEED")
		a.stat.corr("TAVG", "MEAN_SPEED")
		a.stat.corr("WDF2", "MEAN_SPEED")
		a.stat.corr("WSF2", "MEAN_SPEED")
		a.stat.corr("MONTH", "MEAN_SPEED")
		a.stat.corr("YEAR", "MEAN_SPEED")
		a.stat.corr("HOUR", "MEAN_SPEED")


	}
}
