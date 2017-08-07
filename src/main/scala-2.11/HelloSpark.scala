import org.apache.spark.sql.SparkSession


object HelloSpark{
  def main(args:Array[String]){
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val textFile = spark.read.textFile("README.md")
    /*    println(textFile.count())
        println(textFile.first())*/
    println(textFile.filter(line => line.contains("Project")).count())
    import spark.implicits._
    println(textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b)))

    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    println("Word Count：" + wordCounts.show(5))
  }
}
