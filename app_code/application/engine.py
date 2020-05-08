from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, VectorIndexer
from datetime import datetime
from pyspark.sql import Row,SQLContext
# This function first receive borough, speed, weather data from app.py;
# Second, load trained model parameters;
# Third, reconstruct all the data into correct test data format
# predicr risk level
# return risk level
def predict(sc, borough, speed, weather):
    sqlContext = SQLContext(sc)
    isSpeeding = 0
    if int(speed) > 70:
    	isSpeeding = 1
    model = PipelineModel.load("data/treeModelNew2")
    time = str(datetime.now()).split(' ')
    month = float(time[0].split('-')[1])
    hour = float(time[1].split(':')[0])
    city2id = {"Brooklyn": 0, "Queens": 1,"Staten_Island": 2,"Bronx":3,"Manhattan":4}
    if hour == 0.0:
        hour = 24.0
    data = [(float(city2id[borough]),float(speed),month,hour,float(weather['wind']),float(weather['rain']),float(weather['snow']),float(weather['snwd']),float(weather['temp']*9.0/5.0+32))]

    rdd = sc.parallelize(data)
    test = rdd.map(lambda x: Row(BOROUGH_1=x[0], MEAN_SPEED=x[1],MONTH=x[2],HOUR=x[3],AWND=x[4],PRCP=x[5],SNOW=x[6],SNWD=x[7],TAVG=x[8]))
   
    df = sqlContext.createDataFrame(test)
    df.show()

    assembler = VectorAssembler(
        inputCols=["BOROUGH_1", "MEAN_SPEED", "MONTH", "HOUR", "AWND", "PRCP", "SNOW", "SNWD", "TAVG"],
        outputCol="features")

    df2 = assembler.transform(df)

    df3 = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=25).fit(df2).transform(df2)

    predictions = model.transform(df3)

    predictions.show()

    predictions = predictions.toPandas()

    predictedLevel = predictions["prediction"][0] + isSpeeding

    print("======================")
    print("======================")
    print("predictedLevel:", predictedLevel)
    print("======================")
    print("======================")

    return predictedLevel




