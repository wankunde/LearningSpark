package performance

import java.util.concurrent.{Callable, Executors}

import com.hadoop.compression.lzo.LzopCodec
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 从一个Hive表中查询数据，
  * 1、单线程方式将记录进行两次save操作
  * 2、多线程同时进行两次save操作
  * 进行对比
  * Created by jgh on 2017/8/30 030.
  */
object JobWithMultiThread {
  val hdfsPath = "hdfs://ns1/work/bw/tech/test"

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("MultiJobWithThread")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    val df = getInfo(hiveContext)

    //没有多线程处理的情况，连续执行两个Action操作，生成两个Job
    val t1 = System.currentTimeMillis()
    df.rdd.saveAsTextFile(hdfsPath + "testfile1", classOf[LzopCodec])
    df.rdd.saveAsTextFile(hdfsPath + "testfile2", classOf[LzopCodec])
    val t2 = System.currentTimeMillis()
    println("没有多线程处理两个不相关Job的情况耗时：" + (t2-t1))

    //用Executor实现多线程方式处理Job
    val executorService = Executors.newFixedThreadPool(2)//线程池两个线程
    executorService.submit(new Callable[Unit](){
      @Override
      def call() : Unit = {
        df.rdd.saveAsTextFile(hdfsPath + "testfile3", classOf[LzopCodec])
      }
    })

    executorService.submit(new Callable[Unit](){
      @Override
      def call() : Unit = {
        df.rdd.saveAsTextFile(hdfsPath + "testfile4", classOf[LzopCodec])
      }
    })

    def getInfo(hiveContext : HiveContext) : DataFrame = {
      val sql = "select * from common.dict_hotel_ol"
      hiveContext.sql(sql)
    }
  }
}
