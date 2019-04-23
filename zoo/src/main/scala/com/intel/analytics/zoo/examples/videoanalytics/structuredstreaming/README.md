# Image Producer Using OpenCV and Kafka
This code uses OpenCV for extracting images from video and streaming it through Kafka producer. 

## Download Analytics zoo

Clone the analytics zoo from the branch feature/video-analytics and build locally (mvn clean install -DskipTests=true)

## Running the example 


```bash
export PATH=$PATH:${SPARK_HOME}/bin 

LOGDIR=... // Directory where the log4j.properties files is maintained 
SPARK_HOME=... // the root directory of spark
MASTER=... // local[*] or spark://host-ip:port
JARDIR=... // location of the built JAR
STREAMING_PROP=... // absolute path of the streaming properties files.Please change the paths and parameters in the streaming properties files before execution
MODEL_PATH=... // absolute path of the model to be used

spark-submit \
--master ${MASTER} \
--total-executor-cores 16 \
--driver-memory 8g \
--executor-memory 8g \
--verbose \
--conf spark.executor.cores=4 \
--conf spark.driver.maxResultSize=10G \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--driver-java-options "-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--class com.intel.analytics.zoo.examples.videoanalytics.ImageProducerStructured \
${JARDIR}/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-SNAPSHOT-jar-with-dependencies.jar \
--file ${STREAMING_PROP} \
--modelPath ${MODEL_PATH}
```


