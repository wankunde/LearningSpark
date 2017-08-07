package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

//
// Create a DataFrame based on an RDD of case class objects and perform some basic
// DataFrame operations. The DataFrame can instead be created more directly from
// the standard building blocks -- an RDD[Row] and a schema -- see the example
// FromRowsAndSchema.scala to see how to do that.
//
object Basic {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-Basic")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.0, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
//    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()
    val customerDS = spark.sparkContext.parallelize(custs, 4).toDS()


    // 每个字段的字段类型
//    for(x <- customerDS.schema.fields){
//      println(x.dataType.isInstanceOf[StringType])
//    }

    //求一列值，最大长度
    println(customerDS.select("name").map(s => s.get(0).toString().length).reduce((a, b) => if(a > b) a else b))

 /*   println("*** toString() just gives you the schema")

    println(customerDF.toString())

    println("*** It's better to use printSchema()")

    customerDF.printSchema()

    println("*** show() gives you neatly formatted data")

    customerDF.show()

    println("*** use select() to choose one column")

    customerDF.select("id").show()

    println("*** use select() for multiple columns")

    customerDF.select("sales", "state").show()

    println("*** use filter() to choose rows")

    customerDF.filter($"state".equalTo("CA")).show()
*/
/*    // 按某个字段 计算distinct count
    println(customerDF.dropDuplicates(Seq("id")).count())

    // 处理枚举类型
    customerDF.groupBy("state").count().show()*/

    // max min
//    customerDF.agg("id" -> "max", "sales" -> "min").show()
  }
}
