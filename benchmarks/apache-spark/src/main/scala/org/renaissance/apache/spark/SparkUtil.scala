package org.renaissance.apache.spark

import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait SparkUtil {

  val portAllocationMaxRetries: Int = 16

  val winUtils = "/winutils.exe"

  val gpuRapidsJars : Seq[String]= Seq( "/opt/sparkRapidsPlugin/cudf-0.14-cuda10-1.jar",
                                        "/opt/sparkRapidsPlugin/rapids-4-spark_2.12-0.1.0.jar")

  def setUpSparkContext(
    dirPath: Path,
    threadsPerExecutor: Int,
    benchName: String
  ): SparkContext = {
    setUpHadoop(dirPath)
    val conf = new SparkConf()
      .setAppName(benchName)
      .setMaster(s"local[$threadsPerExecutor]") // changed to single cpu thread
      .set("spark.local.dir", dirPath.toString)
      .set("spark.port.maxRetries", portAllocationMaxRetries.toString)
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.sql.warehouse.dir", dirPath.resolve("warehouse").toString)
      // Adding RAPIDS GPU confs
      .set("spark.sql.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.incompatibleOps.enabled", "true")
      .set("spark.executor.instances", "1") // changed to 1 executor
      .set("spark.executor.cores", "1")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.extraClassPath", gpuRapidsJars(0)+":"+gpuRapidsJars(1))
      .set("spark.rapids.sql.concurrentGpuTasks", "1")
      .set("spark.rapids.memory.pinnedPool.size", "2G")
      .set("spark.locality.wait", "0s")
      .set("spark.sql.files.maxPartitionBytes", "512m")
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .setJars(gpuRapidsJars)
      // Log events to launch sparkGUI log in browser
      //.set("spark.eventLog.enabled", "true")
      //.set("spark.eventLog.dir",  
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc
  }

  def tearDownSparkContext(sc: SparkContext): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  /**
   * For Spark version on renaissance (2.0.0) the Hadoop version is 2.2.0
   * For Windows, the binary zip is not included in the dependencies
   * We include in the resource winutils.exe from
   * https://github.com/srccodes/hadoop-common-2.2.0-bin
   * If Spark version is updated in future releases of renaissance,
   * the file must be upgraded to the corresponding Hadoop version.
   */
  def setUpHadoop(tempDirPath: Path): Any = {
    if (sys.props.get("os.name").toString.contains("Windows")) {
      val winutilsPath = Paths.get(tempDirPath.toAbsolutePath + "/bin")
      Files.createDirectories(winutilsPath)
      IOUtils.copy(
        this.getClass.getResourceAsStream(winUtils),
        new FileOutputStream(winutilsPath.toString + winUtils)
      )
      System.setProperty("hadoop.home.dir", tempDirPath.toAbsolutePath.toString)
    }
  }

  def ensureCaching[T](rdd: RDD[T]): Unit = {
    if (!rdd.getStorageLevel.useMemory) {
      throw new Exception("Spark RDD must be cached !")
    }
  }
}
