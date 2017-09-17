
import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by jointwisdom on 2017/8/10 010.
  */
object ChanceConversion {
  class DimensionInfo(var _city:Int, var _zone:Int, var _star:Int) extends Serializable {
    def city = _city
    def zone = _zone
    def star = _star
  }
  def main(args: Array[String]) {
    val dict_hotel_path="./src/test/resources/data/dict_hotelinfo/20170512/000000_0"
    //    val statisticByHotel="src/test/resources/data/statisticsByHotel/20170526/part-00000.lzo"
    val statisticByHotel="src/test/resources/data/statisticsByHotel/20170526/1111.txt"

    val cityArray = Array(1148, 3277, 1233, 3966, 1560, 237, 377, 1100, 1097, 1597, 28, 267, 370, 514, 345, 1371, 355, 544, 91, 94, 95, 494, 7678, 20967, 21883)

    val conf = new SparkConf().setAppName("Chance_Conversion").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val hiveContext = new  HiveContext(sc)

    //按textFile方式加载文件
    //数据格式：1927888	0	0	舒城中大住宿	0	22249	-1	-1	鼓楼街中大社区县中医院北面	0	\N	\N	\N	0	-1.0	-1.0	舒城中大住宿位于鼓楼街中大社区县中医院北面，邻近文化广场、龙头塔，旅游出行便利。客房通风洁净，配设24小时热水、独立卫浴等设施，宽带让你工作、休闲两不误。这里可以携带宠物，不必担心出门期间爱宠无人照料。		1	20160830	10	HTL
    val hotelInfoTextFile = sc.textFile(dict_hotel_path)
    //按lzoTextInputFormat加载数据文件
//    val statisticLzoFile = sc.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](statisticByHotel)
    val statisticLzoFile = sc.textFile(statisticByHotel)


    val statisticInfoPair  = statisticLzoFile.map(line => {
      val fields: Array[String] = line.split("\t")
      new Pair(fields(2), line)
    })
//    val arrivalStartDate = "20170818"
//    val arrivalEndDate = "20170824"
//    val dtStart = "20170520"
//    val dtEnd = "20170818"
//    val statisticDF = hiveContext.sql("select vendormhid,exposurenum,scannum,ordernum from bwbi_prod_chance2.transform_statisticbyhotel2" +
//      " where arrivaldate > " + arrivalStartDate + " and arrivaldate <= " + arrivalEndDate +
//      " and dt >= " + dtStart + " and dt <= " + dtEnd)

//    statisticDF.map(row => new Pair(row(0).toString, row))


    println(hotelInfoTextFile.first())

    val hotelInfoPair = hotelInfoTextFile.filter(line=>{
      val fields: Array[String] = line.split("\t")
      if(cityArray.contains(fields(5).toInt)){//过滤不需要的城市
        true
      } else {
        false
      }
    }).map(line=>{
      val fields: Array[String] = line.split("\t")
      val d = new DimensionInfo(fields(0).toInt,fields(0).toInt, fields(0).toInt)
      new Pair(fields(0), d)
    })
    println(hotelInfoTextFile.count())
    println(hotelInfoPair.first())

//    hotelInfoPair.join(statisticInfoPair)
//    println(hotelInfoPair.join(statisticInfoPair).first())

  }
}



