// BDAD HW09 
// Chenhao Pan

// conduct the following scala code in spark2-shell on dumbo
// :load /home/cp2390/Profiling.scala

// ---- for original data
// read input csv files
val df = spark.read.format("csv").option("header", "true").load("/user/cp2390/BDAD/project/climateData/*.csv")

// define the data type of each column
val dfType = df.select(df("STATION"), df("NAME"), df("LATITUDE").cast("float"), df("LONGITUDE").cast("float"), df("ELEVATION").cast("float"), df("DATE"),
df("AWND").cast("float"), df("AWND_ATTRIBUTES"), df("PRCP").cast("float"), df("PRCP_ATTRIBUTES"), df("SNOW").cast("float"), df("SNOW_ATTRIBUTES"), 
df("SNWD").cast("float"), df("SNWD_ATTRIBUTES"), df("TAVG").cast("float"), df("TAVG_ATTRIBUTES"), df("TMAX").cast("float"), df("TMAX_ATTRIBUTES"), 
df("TMIN").cast("float"), df("TMIN_ATTRIBUTES"), df("WDF2").cast("float"), df("WDF2_ATTRIBUTES"), df("WDF5").cast("float"), df("WDF5_ATTRIBUTES"), 
df("WSF2").cast("float"), df("WSF2_ATTRIBUTES"), df("WSF5").cast("float"), df("WSF5_ATTRIBUTES"))

// get genral information of the dataset

// count number
dfType.count()

// show schema
dfType.printSchema()

// calculate the max, min, count and stddev for each column
dfType.describe().show()

// calculate the maximun length of each column
dfType.agg(max(length($"STATION"))).as[Int].first
dfType.agg(max(length($"NAME"))).as[Int].first
dfType.agg(max(length($"LATITUDE"))).as[Int].first
dfType.agg(max(length($"LONGITUDE"))).as[Int].first
dfType.agg(max(length($"ELEVATION"))).as[Int].first
dfType.agg(max(length($"DATE"))).as[Int].first
dfType.agg(max(length($"AWND"))).as[Int].first
dfType.agg(max(length($"PRCP"))).as[Int].first
dfType.agg(max(length($"SNOW"))).as[Int].first
dfType.agg(max(length($"SNWD"))).as[Int].first
dfType.agg(max(length($"TAVG"))).as[Int].first
dfType.agg(max(length($"TMAX"))).as[Int].first
dfType.agg(max(length($"TMIN"))).as[Int].first
dfType.agg(max(length($"WDF2"))).as[Int].first
dfType.agg(max(length($"WDF5"))).as[Int].first
dfType.agg(max(length($"WSF2"))).as[Int].first
dfType.agg(max(length($"WSF5"))).as[Int].first

// calculate the minimun length of each column
dfType.agg(min(length($"STATION"))).as[Int].first
dfType.agg(min(length($"NAME"))).as[Int].first
dfType.agg(min(length($"LATITUDE"))).as[Int].first
dfType.agg(min(length($"LONGITUDE"))).as[Int].first
dfType.agg(min(length($"ELEVATION"))).as[Int].first
dfType.agg(min(length($"DATE"))).as[Int].first
dfType.agg(min(length($"AWND"))).as[Int].first
dfType.agg(min(length($"PRCP"))).as[Int].first
dfType.agg(min(length($"SNOW"))).as[Int].first
dfType.agg(min(length($"SNWD"))).as[Int].first
dfType.agg(min(length($"TAVG"))).as[Int].first
dfType.agg(min(length($"TMIN"))).as[Int].first
dfType.agg(min(length($"TMIN"))).as[Int].first
dfType.agg(min(length($"WDF2"))).as[Int].first
dfType.agg(min(length($"WDF5"))).as[Int].first
dfType.agg(min(length($"WSF2"))).as[Int].first
dfType.agg(min(length($"WSF5"))).as[Int].first

// calculate the average length of each column
dfType.agg(avg(length($"STATION"))).as[Double].first
dfType.agg(avg(length($"NAME"))).as[Double].first
dfType.agg(avg(length($"LATITUDE"))).as[Double].first
dfType.agg(avg(length($"LONGITUDE"))).as[Double].first
dfType.agg(avg(length($"ELEVATION"))).as[Double].first
dfType.agg(avg(length($"DATE"))).as[Double].first
dfType.agg(avg(length($"AWND"))).as[Double].first
dfType.agg(avg(length($"PRCP"))).as[Double].first
dfType.agg(avg(length($"SNOW"))).as[Double].first
dfType.agg(avg(length($"SNWD"))).as[Double].first
dfType.agg(avg(length($"TAVG"))).as[Double].first
dfType.agg(avg(length($"TAVG"))).as[Double].first
dfType.agg(avg(length($"TMIN"))).as[Double].first
dfType.agg(avg(length($"WDF2"))).as[Double].first
dfType.agg(avg(length($"WDF5"))).as[Double].first
dfType.agg(avg(length($"WSF2"))).as[Double].first
dfType.agg(avg(length($"WSF5"))).as[Double].first

// ---- for cleaned data
// read input csv files
val df2 = spark.read.format("csv").option("header", "true").load("/user/cp2390/BDAD/project/weather_etl_new/*.csv")

// define the data type of each column
val df2Type = df2.select(df2("BOROUGH"), df2("DATE"), 
df2("AWND").cast("float"), df2("PRCP").cast("float"), df2("SNOW").cast("float"),  df2("SNWD").cast("float"), 
df2("TAVG").cast("float"), df2("TMAX").cast("float"), df2("TMIN").cast("float"), 
df2("WDF2").cast("float"), df2("WDF5").cast("float"), df2("WSF2").cast("float"), df2("WSF5").cast("float"))

// count number
df2Type.count()

// show schema
df2Type.printSchema()

// calculate the max, min, count and stddev for each column
df2Type.describe().show()
