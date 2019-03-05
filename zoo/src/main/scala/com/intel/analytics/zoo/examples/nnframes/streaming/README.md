## Image Inference Streaming example - with Dataframes
This example illustrates how to ingest images through a spark socket connector and pass it through an image inferencing pipeline

## Download Analytics Zoo
You will need to use the analytics zoo version from the branch feature/streaming and build locally (mvn clean install -DskipTests=true)
## Run the example
### Download the pre-trained model
You can download pre-trained models from [Image Classification](https://github.com/intel-analytics/analytics-zoo/blob/master/docs/docs/ProgrammingGuide/image-classification.md)

### Prepare predict dataset
Put your image data for prediction in one folder.

### Run this to start streaming images through an IP and port
```shell
IMAGEDIR=... // the folder in which images are stored
HOST_IP=... // IP of the host on which the streaming application streams images
PORT=... // Port through which the images are streamed
JARDIR=... // location of the built JAR
DELAY=... // delay between consecutive images sent

java -cp ${JARDIR}/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-SNAPSHOT-jar-with-dependencies-and-spark.jar com.intel.analytics.zoo.examples.nnframes.streaming.ImageSocketProducer --host ${HOST_IP} --port ${PORT} --imageFolder ${IMAGEDIR} --txDelay ${DELAY}
```

### Run this example to start ingestion followed by inferencing
Run the following command for Spark local mode (MASTER=local[*]) or cluster mode:
```bash
export PATH=$PATH:${SPARK_HOME}/bin 

LOGDIR=... // Directory where the log4j.properties files is maintained (sample log properties file in zoo/src/resources/)
SPARK_HOME=.. // the root directory of spark
MASTER=... // local[*] or spark://driver-ip:port
MODELPATH=... // model path. Local file system/HDFS/Amazon S3 are supported
JARDIR=... // location of the built JAR
MODE= ... // "local" or "distributed"
HOST_IP=... // IP of the host on which the streaming application streams images
PORT=... // Port through the images are streamed
BATCHDURATION=... // Microbatch duration in milliseconds

spark-submit \
--master ${MASTER} \
--driver-memory 8g \
--executor-memory 8g \
--verbose \
--conf spark.executor.cores=4 \
--conf spark.shuffle.memoryFraction=0 \
--conf spark.streaming.blockInterval=3200ms \
--conf spark.driver.maxResultSize=10G \
--conf spark.shuffle.memoryFraction=0 \
--conf spark.network.timeout=10000000 \
--total-executor-cores 4 \
--driver-java-options "-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--class com.intel.analytics.zoo.examples.nnframes.streaming.StreamInference ${JARDIR}/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-SNAPSHOT-jar-with-dependencies-and-spark.jar --model $MODELPATH/analytics-zoo_resnet-50_imagenet_0.1.0.model --batchSize 4 --partition 32 --host ${HOST_IP} --port ${PORT} --batchDuration ${BATCHDURATION} --mode ${MODE}
```
