import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkContext, SparkConf}


object SparkLzoFile{
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Spark_Lzo_File")
    val sc = new SparkContext(conf)
    //文件路径
    val filePath = "/wh/source/hotel.2017-08-07.txt_10.10.16.105_20170807.lzo"

    //按textFile方式加载文件
    val textFile = sc.textFile(filePath)
    //按lzoTextInputFormat加载数据文件
    val lzoFile = sc.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](filePath)

    println(textFile.partitions.length)// 输出 1
    println(lzoFile.partitions.length)//输出 11

    //两种方式计算word count查看后台任务
    lzoFile.map(_._2.toString).flatMap(x=>x.split("-")).map((_,1)).reduceByKey(_+_).collect

    textFile.flatMap(x=>x.split("\t")).map((_,1)).reduceByKey(_+_).collect

  }
}
