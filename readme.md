# README

## Project Info
This is the project for BDAD Spring 2020 developed on the platform - NYU Dumbo cluster.

#### Project Name: NYC Motor Vehicle Collision Analysis and Prediction

#### Team Member: Mingxi Chen, Juan Li, Chenhao Pan

#### Work Assignment:
1. Cleaning and Profiling data
	- Traffic Speed Data: Mingxi Chen
	- Weather Data: Chenhao Pan
	- Motor Vehicle Collision - Crashes: Juan Li
2. Analysis
	- Join 3 datasets and reformat for further analysis: Mingxi Chen, Chenhao Pan Juan Li
	- Model selection and features analysis: Mingxi Chen, Chenhao Pan, Juan Li
3. Application
	- Frontend: Juan Li
	- Backend: Mingxi Chen
4. Paper
	- Chenhao Pan, Mingxi Chen, Juan Li
5. Presentation Slides
	- Chenhao Pan
6. README
	- Mingxi Chen, Chenhao Pan, Juan Li

#### Tools Used:
- Scala and Spark Tools
- SparkSQL
- MLlib
- Tableau
- Flask

#### Analytic and Inferences:
**Insights:**

1. As for feature importance, HOUR has highest importance value, BOROUGH is the second one, MONTH and AWND are the third and forth important feature

2. The number of collision records increases exponentially after 6AM and reaches to the first peak value at 8AM. This consists with most citizens’ life routine. At 5PM, another peak value appears when most people are off duty or leave school. There is an extra point needs notice, around 9PM, the number of collision records goes up again, especially, risk level 1 and 2 both get to their highest value. This situation may cause by the lack of light, drunk driving and fatigue driving which will often come up with more serious consequences.

3. Queens has more collision records than other boroughs

4. High speed leads to high mortality.

5. As for temperature, one peak value of collision records appears around 76°F, which is near the most comfortable temperature for human. More people  may choose to go outside and travel during that period. This could also lead to hidden danger.

#### References
1. “MLlib: Apache Spark.” MLlib | Apache Spark. Accessed May 5, 2020. https://spark.apache.org/mllib/.
2. “Application Setup.” Application Setup - Flask Documentation (1.1.x). Accessed May 5, 2020. https://flask.palletsprojects.com/en/1.1.x/tutorial/factory/.

## Cleaning and Profiling

### Clean speed data:

#### 2 steps

1. **PATH:**

	/home/mc7805/speed\_code/clean\_profile/src/main/scala/clean\_csv.scala

	**HOW TO RUN:**

	cd speed\_code/clean\_profile

	spark2-submit --class Clean --packages com.databricks:spark-csv\_2.11:1.5.0 /home/mc7805/speed\_code/clean\_profile/target/scala-2.11/my-project\_2.11-1.0.jar

	**WHERE TO FIND RESULTS:**

	hdfs:///user/mc7805/finalProject/processed\_real\_time\_speed\_2.csv

2. **PATH:**

	/home/mc7805/speed\_code/extra_clean.scala

	**HOW TO RUN:**

	This file collects all used commands. Please use spark2-shell

	**WHERE TO FIND RESULTS:**

	hdfs:///user/mc7805/data/speed\_data\_new

### Profile speed data:

1. **PATH:**

	/home/mc7805/speed\_code/clean\_profile/src/main/scala/profile.scala

	**HOW TO RUN:**

	cd speed\_code/clean\_profile

	spark2-submit --class Profile --packages com.databricks:spark-csv\_2.11:1.5.0 /home/mc7805/speed\_code/clean\_profile/target/scala-2.11/my-project\_2.11-1.0.jar

	**WHERE TO FIND RESULTS:**

	The results should be printed in the command line.

### Clean weather data:

1. **PATH:**

    /home/cp2390/ETL\_new.scala

    **HOW TO RUN:**

    This file collects all used commands. Please use spark2-shell

    spark2-shell

    :load  /home/cp2390/ETL\_new.scala

    **WHERE TO FIND RESULTS:**

    hdfs:///user/cp2390/BDAD/project/weather\_etl\_new

    hdfs:///user/mc7805/data/weather\_etl\_new

### Profile weather data

1. **PATH:**

    /home/cp2390/Profiling.scala

    **HOW TO RUN:**

    This file collects all used commands. Please use spark2-shell

    spark2-shell

    :load  /home/cp2390/Profiling.scala

    **WHERE TO FIND RESULTS:**

    The results should be printed in the command line.

### Clean collision data:

1. **PATH:**

   /home/jl10889/clean.scala

   **HOW TO RUN:**

   This file collects all used commands. Please use spark2-shell

   spark2-shell

   :load  /home/jl10889/clean.scala

   **WHERE TO FIND RESULTS:**

   hdfs:///user/jl10889/data.csv

   hdfs:///user/mc7805/data/data-collision-new

### Profile collision data

1. **PATH:**

   /home/jl10889/profiling.scala

   **HOW TO RUN:**

   This file collects all used commands. Please use spark2-shell

   spark2-shell

   :load  /home/jl10889/profiling.scala

   **WHERE TO FIND RESULTS:**

   The results should be printed in the command line.


## Analysis

### 2 steps

1. **TASK:**

	Join 3 datasets and reformat for further analysis

	**PATH:**

	/home/mc7805/project\_code/src/main/scala/buildData.scala

	**HOW TO RUN:**

	cd project_code

	spark2-submit --name buildData --class BuildData target/scala-2.11/*.jar
	
	**WHERE TO FIND RESULTS:**
	
	hdfs:///user/mc7805/data/train_data_latest_version_4

2. **TASK:**

	Model selection and features analysis

	**PATH:**

	/home/mc7805/project\_code/src/main/scala/modelSelection.scala

	**HOW TO RUN:**

	cd project_code

	spark2-submit --name modelSelection --class Model target/scala-2.11/*.jar
	
	**WHERE TO FIND RESULTS:**
	
	hdfs:///user/mc7805/data/treeModelNew2

## Application

### STRUCTURE
**server.py** *Init spark context, load libraries and start flask application*

**app.py** *Receive borough and speed from frontend, receive weather data from api, send data to engine.py*

**engine.py** *Receive borough, speed, weather data from app.py, load trained model parameters,reconstruct all the data into correct test data format, predicr risk level and return risk level*

**static** (CSS/JS)

**templates** (HTML)

### PATH

/home/mc7805/frontend\_new2/

### HOW TO RUN

First, please run the following command in your local terminator which will bind your local machine port 5000 to the remote port 5000:

**ssh -L 5000:127.0.0.1:5000 mc7805@dumbo.hpc.nyu.edu**

Then in dumbo:

**cd /home/mc7805/frontendnew\_2/**

**spark2-submit server.py**

Paste http://127.0.0.1:5000/ to your local browser

Select borough and speed you'd like to check, click submit to see predicted risk level.
