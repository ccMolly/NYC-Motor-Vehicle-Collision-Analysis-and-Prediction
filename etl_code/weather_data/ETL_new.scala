// BDAD HW09 
// Chenhao Pan

// conduct the following scala code in spark2-shell on dumbo
// :load /home/cp2390/ETL_new.scala

import scala.math.{abs, max, min}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

// QUEENS
// ==================================================
//   {'x': -73.82998999999995, 'y': 40.714000000000055}
// MANHATTAN
// ==================================================
//   {'x': -74.00600999999995, 'y': 40.714500000000044}
// STATEN ISLAND
// ==================================================
//   {'x': -74.07526999999999, 'y': 40.64242000000007}
// BRONX
// ==================================================
//   {'x': -73.92308999999995, 'y': 40.82600000000008}
// BROOKLYN
// ==================================================
//   {'x': -73.99035999999995, 'y': 40.692450000000065}


// functions to define coordinate ranges for each boroguh and return the borough name
def getStaten(array: Seq[Any]) : String = 
{ 
	var borough: String = ""
	if (array(1).asInstanceOf[Float].abs < 40.72 && array(1).asInstanceOf[Float].abs > 40.41 &&array(2).asInstanceOf[Float].abs < 74.33 && array(2).asInstanceOf[Float].abs > 73.98) {
		borough = "Staten Island"	
	}
	return borough
}

def getManhattan(array: Seq[Any]) : String = 
{ 
	var borough: String = ""
	if (array(1).asInstanceOf[Float].abs < 40.95 && array(1).asInstanceOf[Float].abs > 40.55 &&array(2).asInstanceOf[Float].abs < 74.2 && array(2).asInstanceOf[Float].abs > 73.7) {
		borough = "Manhattan"	
	}
	return borough
}

def getBronx(array: Seq[Any]) : String = 
{ 
	var borough: String = ""
	if (array(1).asInstanceOf[Float].abs < 40.97 && array(1).asInstanceOf[Float].abs > 40.71 &&array(2).asInstanceOf[Float].abs < 74.01 && array(2).asInstanceOf[Float].abs > 73.69) {
		borough = "Bronx"	
	}
	return borough
}

def getQueens(array: Seq[Any]) : String = 
{ 
	var borough: String = ""
	if (array(1).asInstanceOf[Float].abs < 40.85 && array(1).asInstanceOf[Float].abs > 40.45 &&array(2).asInstanceOf[Float].abs < 74.01 && array(2).asInstanceOf[Float].abs > 73.62) {
		borough = "Queens"	
	}
	return borough
}

def getBrooklyn(array: Seq[Any]) : String = 
{ 
var borough: String = ""
	if (array(1).asInstanceOf[Float].abs < 40.81 && array(1).asInstanceOf[Float].abs > 40.49 &&array(2).asInstanceOf[Float].abs < 74.12 && array(2).asInstanceOf[Float].abs > 73.76) {
		borough = "Brooklyn"	
	}
	return borough
}

// read input csv files
val df = spark.read.format("csv").option("header", "true").load("/user/cp2390/BDAD/project/climateData/*.csv")

// define the data type of each column
val df2 = df.select(df("STATION"), df("NAME"), df("LATITUDE").cast("float"), df("LONGITUDE").cast("float"), df("ELEVATION").cast("float"), df("DATE").cast("date"),
df("AWND").cast("float"), df("AWND_ATTRIBUTES"), df("PRCP").cast("float"), df("PRCP_ATTRIBUTES"), df("SNOW").cast("float"), df("SNOW_ATTRIBUTES"), 
df("SNWD").cast("float"), df("SNWD_ATTRIBUTES"), df("TAVG").cast("float"), df("TAVG_ATTRIBUTES"), df("TMAX").cast("float"), df("TMAX_ATTRIBUTES"), 
df("TMIN").cast("float"), df("TMIN_ATTRIBUTES"), df("WDF2").cast("float"), df("WDF2_ATTRIBUTES"), df("WDF5").cast("float"), df("WDF5_ATTRIBUTES"), 
df("WSF2").cast("float"), df("WSF2_ATTRIBUTES"), df("WSF5").cast("float"), df("WSF5_ATTRIBUTES"))

// change the date format from yyyy-MM-dd to MM/dd/yyyy
val dfType = df2.withColumn("DATE",date_format(to_date(col("DATE"), "yyyy-MM-dd"), "MM/dd/yyyy"))

// getStaten
val AWND1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "AWND", "AWND_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val PRCP1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "PRCP", "PRCP_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNOW1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNOW", "SNOW_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNWD1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNWD", "SNWD_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TAVG1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TAVG", "TAVG_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMAX1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMAX_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMIN1 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMIN", "TMIN_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF21 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF2", "WDF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF51 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF5", "WDF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF21 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF2", "WSF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF51 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF5", "WSF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getStaten(line) != "")).map(line => ((getStaten(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }

// getBrooklyn
val AWND2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "AWND", "AWND_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val PRCP2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "PRCP", "PRCP_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNOW2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNOW", "SNOW_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNWD2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNWD", "SNWD_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TAVG2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TAVG", "TAVG_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMAX2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMAX_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMIN2 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMIN", "TMIN_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF22 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF2", "WDF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF52 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF5", "WDF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF22 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF2", "WSF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF52 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF5", "WSF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBrooklyn(line) != "")).map(line => ((getBrooklyn(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }

// getQueens
val AWND3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "AWND", "AWND_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val PRCP3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "PRCP", "PRCP_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNOW3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNOW", "SNOW_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNWD3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNWD", "SNWD_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TAVG3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TAVG", "TAVG_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMAX3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMAX_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMIN3 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMIN", "TMIN_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF23 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF2", "WDF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF53 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF5", "WDF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF23 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF2", "WSF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF53 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF5", "WSF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getQueens(line) != "")).map(line => ((getQueens(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }

// getManhattan
val AWND4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "AWND", "AWND_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val PRCP4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "PRCP", "PRCP_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNOW4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNOW", "SNOW_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNWD4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNWD", "SNWD_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TAVG4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TAVG", "TAVG_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMAX4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMAX_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMIN4 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMIN", "TMIN_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF24 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF2", "WDF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF54 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF5", "WDF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF24 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF2", "WSF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF54 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF5", "WSF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getManhattan(line) != "")).map(line => ((getManhattan(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }

// getBronx
val AWND5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "AWND", "AWND_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val PRCP5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "PRCP", "PRCP_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNOW5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNOW", "SNOW_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val SNWD5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "SNWD", "SNWD_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TAVG5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TAVG", "TAVG_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMAX5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMAX", "TMAX_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val TMIN5 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "TMIN", "TMIN_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF25 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF2", "WDF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WDF55 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WDF5", "WDF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF25 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF2", "WSF2_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }
val WSF55 = dfType.select("DATE", "LATITUDE", "LONGITUDE", "WSF5", "WSF5_ATTRIBUTES").rdd.map(_.toSeq).filter(line => line(4) != null).filter(line => (getBronx(line) != "")).map(line => ((getBronx(line), line(0)), line(3).asInstanceOf[Float])).mapValues(value => (value, 1)).reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR)}.mapValues { case (sum , count) => sum / count }

// join different attributes rdds and combiine them
val joinRDD1 = AWND1.join(PRCP1).join(SNOW1).join(SNWD1).join(TAVG1).join(TMAX1).join(TMIN1).join(WDF21).join(WDF51).join(WSF21).join(WSF51)
val joinRDD2 = AWND2.join(PRCP2).join(SNOW2).join(SNWD2).join(TAVG2).join(TMAX2).join(TMIN2).join(WDF22).join(WDF52).join(WSF22).join(WSF52)
val joinRDD3 = AWND3.join(PRCP3).join(SNOW3).join(SNWD3).join(TAVG3).join(TMAX3).join(TMIN3).join(WDF23).join(WDF53).join(WSF23).join(WSF53)
val joinRDD4 = AWND4.join(PRCP4).join(SNOW4).join(SNWD4).join(TAVG4).join(TMAX4).join(TMIN4).join(WDF24).join(WDF54).join(WSF24).join(WSF54)
val joinRDD5 = AWND5.join(PRCP5).join(SNOW5).join(SNWD5).join(TAVG5).join(TMAX5).join(TMIN5).join(WDF25).join(WDF55).join(WSF25).join(WSF55)
val unionRDD = joinRDD1.union(joinRDD2).union(joinRDD3)union(joinRDD4)union(joinRDD5)
val cleanup = unionRDD.map(line => line.toString.replaceAll("[()]",""))

// define the Header for the dataframe, rdd to dataframe
val structFields = Array(StructField("BOROUGH",StringType,true), StructField("DATE",StringType,true), StructField("AWND",StringType,true), StructField("PRCP",StringType,true), StructField("SNOW",StringType,true), StructField("SNWD",StringType,true), StructField("TAVG",StringType,true), StructField("TMAX",StringType,true), StructField("TMIN",StringType,true), StructField("WDF2",StringType,true), StructField("WDF5",StringType,true), StructField("WSF2",StringType,true), StructField("WSF5",StringType,true))
val structType = StructType(structFields)
val DataFrame = cleanup.map( line => line.split(",")).map(a => Row.fromSeq(a))

// save final dataframe to weather_etl_new folder
val output = spark.createDataFrame(DataFrame,structType)
val path: String = "/user/cp2390/BDAD/project/weather_etl_new"
output.write.option("header", "true").csv(path)

