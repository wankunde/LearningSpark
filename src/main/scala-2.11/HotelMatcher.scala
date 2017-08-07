import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ListBuffer
import scala.util.control._

import com.hankcs.hanlp.tokenizer.NLPTokenizer
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.HanLP
import scala.collection.mutable.ArrayBuffer

object HotelMatcher {
  def main(args:Array[String]){
    if(args.length < 2){
      println("参数错误:qunarFile  ctripFile")
      return
    }
    start(args(0),args(1))
  }
   
  def start(qunarFile: String, ctripFile: String) {
    val spark = SparkSession
      .builder()
      .appName("HotelMacher")
      .getOrCreate()

    val sc = spark.sparkContext
    //id\tcity\tprovince\tname\taddress
    val ctripMap = sc.textFile(ctripFile)
      .map(x => x.split('\t'))
      .map(a => (a(2) + a(1), a)) //province + city
      .aggregateByKey(ListBuffer.empty[Array[String]])((list, a) => { list.append(a); list }, (list1, list2) => { list1.appendAll(list2); list1 })
      .collectAsMap()

    val ctripBc = sc.broadcast(ctripMap)
    
    //id\tcity\tprovince\tname\taddress
    val rdd = sc.textFile(qunarFile)
      .map(x => x.split('\t'))
      .mapPartitions(iter => {
        var qr: Array[String] = Array()
        var list: ListBuffer[Tuple2[Array[String], Array[String]]] = ListBuffer()
        var cp: Array[String] = Array()

        val loop = new Breaks;

        for (r <- iter) {
          val ctrips = ctripBc.value.get(r(2) + r(1)) //province + city
          var flag = false
          if (!ctrips.isEmpty) {
            loop.breakable {
              for (c <- ctrips.get) {
                //名字和地址完全相同，认为是同一家酒店
                if (r(3).equals(c(3)) && r(4).equals(c(4))) {
                  list.append((r, c))
                  flag = true
                  loop.break()
                } else {
                  val trims = Set(r(1), r(2), r(1) + "市", r(2) + "省")

                  val qnName = delInvalid(r(3))
                  val cpName = delInvalid(c(3))

                  val nameDistance = getCosDistance(qnName, cpName, trims)

                  val qnAddr = delInvalid(r(4))
                  val cpAddr = delInvalid(c(4))
                  val addrDistance = getCosDistance(qnAddr, cpAddr, trims)

                  if (nameDistance >= 0.93 && addrDistance >= 0.93) {
                    list.append((r, c))
                    flag = true
                    loop.break()
                  }
                }

              }
            }
          }
        }

        list.iterator
      })

    rdd.foreach(x=>{val s1 = x._1.mkString(","); val s2 = x._2.mkString(",");println(s1 + ">" + s2)})

    spark.stop();
  }

  def delInvalid(s: String): String = {
    val ns = s.trim()
    val p = ns.indexOf('(')
    if (p > 0) {
      return ns.substring(0, p)
    } else {
      val p = ns.indexOf('（')
      if (p > 0) {
        return ns.substring(0, p)
      }
    }
    ns
  }

  def getVectorWords(s: String, trims: Set[String]) = {
    val words = HanLP.segment(s)
    val n = words.size();
    var buf: ArrayBuffer[String] = ArrayBuffer()
    for (i <- 0 until n) {
      if (!trims.contains(words.get(i).word)) {
        buf += words.get(i).word
      }
    }

    buf.toSet
  }

  def getVector(words: Set[String], allWords: Array[String]) = {
    var buf: ArrayBuffer[Int] = ArrayBuffer()
    for (w <- allWords) {
      if (words.contains(w)) {
        buf += 1
      } else {
        buf += 0
      }
    }

    buf.toArray
  }

  def cosDistance(v1: Array[Int], v2: Array[Int]) = {
    val squre1 = v1.map(x => x * x).sum
    val squre2 = v2.map(x => x * x).sum
    val sum = v1.zip(v2).map(r => { r._1 * r._2 }).sum

    sum * 1.0 / (math.sqrt(squre1) * math.sqrt(squre2))

  }

  def getCosDistance(s1: String, s2: String, trims: Set[String]) = {
    val w1 = getVectorWords(s1, trims) //分词
    val w2 = getVectorWords(s2, trims)
    val allWords = (w1 ++ w2).toArray //合并

    val v1 = getVector(w1, allWords) //生成向量
    val v2 = getVector(w2, allWords)

    cosDistance(v1, v2)
  }
}