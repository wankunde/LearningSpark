
import org.apache.spark.sql.{SparkSession}
import org.json4s.JsonDSL._

/**
  * 数据治理项目-表数据质量统计
  */
object DataManagement_TableRule {
  case class StatInfo(colType: String, min: String, max: String, nullCount: Long, notNullCount: Long, maxLength: Int)
  case class TableStatInfo(tableName: String, count: Long, statInfo: List[StatInfo])

  def main(args: Array[String]) {
    // 定义每种处理方式的数据字段
    val distinctField = List("order_id", "hotel_id")
    val enumField = List("pay_type", "device_type")
    val fields = List("discount_total","discount_detail")
    val tableName = "biz_hotelorder"

    val statInfoList = List(StatInfo("colType","1","2", 10, 12, 202), StatInfo("colType","1","2", 10, 12, 202))
    val tableStatInfo = new TableStatInfo("biz_hotelorder", 0 , statInfoList)
    val json1 =
      ("tableStatInfo" ->
        ("tableName" -> tableStatInfo.tableName) ~
          ("itemCount" -> tableStatInfo.count) ~
          ("StatInfo" ->
            tableStatInfo.statInfo.map { w =>
              (("colType" -> w.colType) ~
               ("min" -> w.min) ~
               ("max" -> w.max) ~
               ("nullCount" -> w.nullCount) ~
               ("notNullCount" -> w.notNullCount) ~
               ("maxLength" -> w.maxLength))}))

    val dailyPath = "./src/test/resources/data/parquet/"

    val spark =
      SparkSession.builder()
        .appName("DataFrame-Basic")
        .master("local[2]")
        .getOrCreate()

    val dataset = spark.read.parquet(dailyPath).as("123")

     // 记录数
     /*println(dataset.count())

     // 按字段distinct
     for (x <- distinctField) {
       println(x + " distinct个数：" + dataset.dropDuplicates(Seq(x)).count())
     }

     // 处理枚举类型
     for (x <- enumField) {
       dataset.groupBy(x).count().show()
     }

    // max min isNullCount
    for(x <- fields){
      if(StringUtils.equals(x,"discount_detail")){//TODO 判断列类型，如果是String,则计算最大长度
        import spark.implicits._
        val maxLength = dataset.select(x).map(s => s.get(0).toString().length).reduce((a, b) => if(a > b) a else b)
        println("discount_detail maxLength:" + maxLength)

      } else if(StringUtils.equals(x,"discount_total")){//TODO 判断列类型，如果是Int，则计算min max isNullCount,
        dataset.agg(x -> "max", x -> "min").show()
        println(x + "null Count:" + dataset.filter(dataset.col(x).isNull).count())
        println(x + "not null Count" + dataset.filter(dataset.col(x).isNotNull).count())
      }
    }

    println(compact(render(json1)))*/


    spark.udf.register("maxLength", new MyUDAF)
//    dataset.groupBy("pay_type").agg("pay_type" -> "maxLength", "pay_type" -> "count").show()
    dataset.agg("order_id" -> "maxLength").show()
  }
}
