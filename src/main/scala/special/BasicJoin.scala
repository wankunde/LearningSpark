package special


import org.apache.spark.{SparkContext, SparkConf}

object BasicJoin {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("HashJoin").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val smallRDD = sc.parallelize(Seq(("V1", '1'), ("V2", '2')), 4)

    val largeRDD = sc.parallelize(Seq(("V1", '1'), ("V1", '2')), 4)

//    val joined = largeRDD.join(smallRDD)
    val joined = smallRDD.join(largeRDD)
    joined.collect().foreach(println)
  }
}