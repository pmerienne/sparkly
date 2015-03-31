package sparkly.core


object SparkDefaultConfiguration {

  val defaultBatchDurationMs = 1000L

  val defaultApplicationConfiguraion = Map (
    "spark.app.name" -> "sparkly",
    "spark.executor.memory" -> "512m",
    "spark.driver.memory" -> "512m",
    "spark.driver.maxResultSize" -> "1g",
    "spark.logConf" -> "false"
  )

  val defaultRuntimeConfiguration = Map (
    "spark.driver.extraJavaOptions" -> "",
    "spark.executor.extraJavaOptions" -> "",
    "spark.executor.logs.rolling.strategy" -> "time",
    "spark.executor.logs.rolling.time.interval" -> "daily",
    "spark.executor.logs.rolling.size.maxBytes" -> "",
    "spark.executor.logs.rolling.maxRetainedFiles" -> ""
  )

  val defaultStreamingConfiguration = Map (
    "spark.streaming.blockInterval" -> "200",
    "spark.streaming.receiver.maxRate" -> "0",
    "spark.streaming.receiver.writeAheadLog.enable" -> "true",
    "spark.streaming.unpersist" -> "true"
  )

  val defaultShuffleConfiguration = Map (
    "spark.shuffle.consolidateFiles" -> "false",
    "spark.shuffle.spill" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.shuffle.memoryFraction" -> "0.2",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.file.buffer.kb" -> "32",
    "spark.reducer.maxMbInFlight" -> "48",
    "spark.shuffle.manager" -> "sort",
    "spark.shuffle.sort.bypassMergeThreshold" -> "200",
    "spark.shuffle.blockTransferService" -> "netty"
  )

  val defaultCompressionConfiguration = Map (
    "spark.broadcast.compress" -> "true",
    "spark.rdd.compress" -> "false",
    "spark.io.compression.codec" -> "snappy",
    "spark.io.compression.snappy.block.size" -> "32768",
    "spark.io.compression.lz4.block.size" -> "32768",
    "spark.serializer.objectStreamReset" -> "100",
    "spark.kryo.referenceTracking" -> "true",
    "spark.kryo.registrationRequired" -> "false",
    "spark.kryoserializer.buffer.mb" -> "0.064",
    "spark.kryoserializer.buffer.max.mb" -> "64"
  )

  val defaultExecutionConfiguration = Map (
    "spark.default.parallelism" -> "8",
    "spark.broadcast.factory" -> "org.apache.spark.broadcast.TorrentBroadcastFactory",
    "spark.broadcast.blockSize" -> "4096",
    "spark.files.overwrite" -> "false",
    "spark.files.fetchTimeout" -> "60",
    "spark.storage.memoryFraction" -> "0.6",
    "spark.storage.unrollFraction" -> "0.2",
    "spark.storage.memoryMapThreshold" -> "8192",
    "spark.tachyonStore.url" -> "tachyon://localhost:19998",
    "spark.cleaner.ttl" -> "3600",
    "spark.hadoop.validateOutputSpecs" -> "true",
    "spark.hadoop.cloneConf" -> "false",
    "spark.executor.heartbeatInterval" -> "10000"
  )

  val defaultSchedulingConfiguration = Map (
    "spark.task.cpus" -> "1",
    "spark.task.maxFailures" -> "4",
    "spark.scheduler.mode" -> "FIFO",
    "spark.cores.max" -> "",
    "spark.speculation" -> "false",
    "spark.speculation.interval" -> "100",
    "spark.speculation.quantile" -> "0.75",
    "spark.speculation.multiplier" -> "1.5",
    "spark.locality.wait" -> "3000",
    "spark.locality.wait.process" -> "3000",
    "spark.locality.wait.node" -> "3000",
    "spark.locality.wait.rack" -> "3000",
    "spark.scheduler.revive.interval" -> "1000",
    "spark.scheduler.minRegisteredResourcesRatio" -> "0.0",
    "spark.scheduler.maxRegisteredResourcesWaitingTime" -> "30000",
    "spark.localExecution.enabled" -> "false"
  )

  val defaultAllocationConfiguration = Map (
    "spark.dynamicAllocation.enabled" -> "false",
    "spark.dynamicAllocation.minExecutors" -> "",
    "spark.dynamicAllocation.maxExecutors" -> "",
    "spark.dynamicAllocation.schedulerBacklogTimeout" -> "60",
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout" -> "60",
    "spark.dynamicAllocation.executorIdleTimeout" -> "600"
  )

  val defaultSettings = Map[String, Map[String, String]](
    "Application Properties" -> defaultApplicationConfiguraion,
    "Runtime Environment" -> defaultRuntimeConfiguration,
    "Streaming" -> defaultStreamingConfiguration,
    "Shuffle Behavior" -> defaultShuffleConfiguration,
    "Compression and Serialization" -> defaultCompressionConfiguration,
    "Execution Behavior" -> defaultExecutionConfiguration,
    "Scheduling" -> defaultSchedulingConfiguration,
    "Dynamic allocation" -> defaultAllocationConfiguration
  )

}