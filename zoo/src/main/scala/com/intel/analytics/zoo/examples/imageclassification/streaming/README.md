## Image Classification Streaming example
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
--conf spark.driver.maxResultSize=10G \
--conf spark.shuffle.memoryFraction=0 \
--conf spark.network.timeout=10000000 \
--total-executor-cores 4 \
--driver-java-options "-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$LOGDIR/log4j.properties" \
--class com.intel.analytics.zoo.examples.imageclassification.streaming.StreamPredict ${JARDIR}/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-SNAPSHOT-jar-with-dependencies-and-spark.jar --model $MODELPATH/analytics-zoo_resnet-50_imagenet_0.1.0.model --batchSize 4 --partition 32 --host ${HOST_IP} --port ${PORT} --batchDuration ${BATCHDURATION} --mode ${MODE}
```

### Current Issues faced with this implementation
#### Running this example gives following errors after processing a couple of RDD microbatches in the local mode:

**Error #1**
*java.lang.IllegalArgumentException: requirement failed: input channel size 1 is not the same as nInputPlane 3
	at scala.Predef$.require(Predef.scala:224)
	at com.intel.analytics.bigdl.nn.SpatialConvolution.updateOutput(SpatialConvolution.scala:262)
	at com.intel.analytics.bigdl.nn.SpatialConvolution.updateOutput(SpatialConvolution.scala:54)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:258)
	at com.intel.analytics.bigdl.nn.StaticGraph.updateOutput(StaticGraph.scala:59)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:258)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:66)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:65)
	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)
	at scala.collection.Iterator$$anon$19.hasNext(Iterator.scala:800)
	at scala.collection.Iterator$class.foreach(Iterator.scala:893)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1336)
	at com.intel.analytics.bigdl.optim.Predictor$.predictImageBatch(Predictor.scala:47)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:175)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:171)
	at com.intel.analytics.bigdl.utils.ThreadPool$$anonfun$invokeAndWait$1$$anonfun$apply$2.apply(ThreadPool.scala:102)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:264)
	at com.intel.analytics.bigdl.nn.StaticGraph.updateOutput(StaticGraph.scala:59)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:258)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:66)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:65)
	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)
	at scala.collection.Iterator$$anon$19.hasNext(Iterator.scala:800)
	at scala.collection.Iterator$class.foreach(Iterator.scala:893)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1336)
	at com.intel.analytics.bigdl.optim.Predictor$.predictImageBatch(Predictor.scala:47)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:175)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:171)
	at com.intel.analytics.bigdl.utils.ThreadPool$$anonfun$invokeAndWait$1$$anonfun$apply$2.apply(ThreadPool.scala:102)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)*

**Error #2**
*ERROR com.intel.analytics.bigdl.utils.ThreadPool$ - Error: java.lang.ArrayIndexOutOfBoundsException
	at java.lang.System.arraycopy(Native Method)
	at com.intel.analytics.bigdl.tensor.TensorNumericMath$TensorNumeric$NumericFloat$.arraycopy$mcF$sp(TensorNumeric.scala:721)
	at com.intel.analytics.bigdl.tensor.TensorNumericMath$TensorNumeric$NumericFloat$.arraycopy(TensorNumeric.scala:715)
	at com.intel.analytics.bigdl.tensor.TensorNumericMath$TensorNumeric$NumericFloat$.arraycopy(TensorNumeric.scala:503)
	at com.intel.analytics.bigdl.dataset.MiniBatch$.copy(MiniBatch.scala:460)
	at com.intel.analytics.bigdl.dataset.MiniBatch$.copyWithPadding(MiniBatch.scala:380)
	at com.intel.analytics.bigdl.dataset.ArrayTensorMiniBatch.set(MiniBatch.scala:209)
	at com.intel.analytics.bigdl.dataset.ArrayTensorMiniBatch.set(MiniBatch.scala:111)
	at com.intel.analytics.bigdl.dataset.SampleToMiniBatch$$anon$2.next(Transformer.scala:348)
	at com.intel.analytics.bigdl.dataset.SampleToMiniBatch$$anon$2.next(Transformer.scala:323)
	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)
	at scala.collection.Iterator$$anon$19.hasNext(Iterator.scala:800)
	at scala.collection.Iterator$class.foreach(Iterator.scala:893)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1336)
	at com.intel.analytics.bigdl.optim.Predictor$.predictImageBatch(Predictor.scala:47)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:175)
	at com.intel.analytics.bigdl.optim.LocalPredictor$$anonfun$7$$anonfun$8$$anonfun$apply$5.apply(LocalPredictor.scala:171)
	at com.intel.analytics.bigdl.utils.ThreadPool$$anonfun$invokeAndWait$1$$anonfun$apply$2.apply(ThreadPool.scala:102)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)*

#### Running this example gives following error after processing a couple of RDD microbatches in the distributed mode:

*java.lang.ArrayIndexOutOfBoundsException
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:264)
	at com.intel.analytics.bigdl.nn.Scale.updateOutput(Scale.scala:50)
	at com.intel.analytics.bigdl.nn.Scale.updateOutput(Scale.scala:37)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:258)
	at com.intel.analytics.bigdl.nn.StaticGraph.updateOutput(StaticGraph.scala:59)
	at com.intel.analytics.bigdl.nn.abstractnn.AbstractModule.forward(AbstractModule.scala:258)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:66)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$predictSamples$1.apply(Predictor.scala:65)
	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)
	at scala.collection.Iterator$$anon$19.hasNext(Iterator.scala:800)
	at scala.collection.Iterator$class.foreach(Iterator.scala:893)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1336)
	at com.intel.analytics.bigdl.optim.Predictor$.predictImageBatch(Predictor.scala:47)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$6$$anonfun$apply$1.apply(Predictor.scala:135)
	at com.intel.analytics.bigdl.optim.Predictor$$anonfun$6$$anonfun$apply$1.apply(Predictor.scala:134)
	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)
	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)
	at scala.collection.Iterator$class.foreach(Iterator.scala:893)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1336)
	at scala.collection.generic.Growable$class.$plus$plus$eq(Growable.scala:59)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:104)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:48)
	at scala.collection.TraversableOnce$class.to(TraversableOnce.scala:310)
	at scala.collection.AbstractIterator.to(Iterator.scala:1336)
	at scala.collection.TraversableOnce$class.toBuffer(TraversableOnce.scala:302)
	at scala.collection.AbstractIterator.toBuffer(Iterator.scala:1336)
	at scala.collection.TraversableOnce$class.toArray(TraversableOnce.scala:289)
	at scala.collection.AbstractIterator.toArray(Iterator.scala:1336)
	at org.apache.spark.rdd.RDD$$anonfun$collect$1$$anonfun$12.apply(RDD.scala:939)
	at org.apache.spark.rdd.RDD$$anonfun$collect$1$$anonfun$12.apply(RDD.scala:939)
	at org.apache.spark.SparkContext$$anonfun$runJob$5.apply(SparkContext.scala:2074)
	at org.apache.spark.SparkContext$$anonfun$runJob$5.apply(SparkContext.scala:2074)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87)
	at org.apache.spark.scheduler.Task.run(Task.scala:109)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:345)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)*

