package json

object JsonExample extends App {
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  //第一部分，官网例子
  case class Winner(id: Long, numbers: List[Int])
  case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])

  val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
  val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)

  val json =
    ("lotto1" ->
      ("lotto-id" -> lotto.id) ~
      ("winning-numbers" -> lotto.winningNumbers) ~
      ("draw-date" -> lotto.drawDate.map(_.toString)) ~
      ("winners" ->
        lotto.winners.map { w =>
          (("winner-id" -> w.id) ~
           ("numbers" -> w.numbers))}))

  println(compact(render(json)))

  //第二部分，参照官网例子写的
  case class StatInfo(min: String, max: String, nullCount: Long, notNullCount: Long, maxLength: Int)
  case class TableStatInfo(tableName: String, count: Long, statInfo: List[StatInfo])
  //  val statInfo = StatInfo("1","2", 10, 12, 202)
  val statInfoList = List(StatInfo("1","2", 10, 12, 202), StatInfo("1","2", 10, 12, 202))
  val tableStatInfo = new TableStatInfo("biz_hotelorder", 0 , statInfoList)
  val json1 =
    ("tableStatInfo" ->
      ("tableName" -> tableStatInfo.tableName) ~
        ("itemCount" -> tableStatInfo.count) ~
        ("StatInfo" ->
          tableStatInfo.statInfo.map { w =>
            (("min" -> w.min) ~
              ("max" -> w.max) ~
              ("nullCount" -> w.nullCount) ~
              ("notNullCount" -> w.notNullCount) ~
              ("maxLength" -> w.maxLength))}))

  println(compact(render(json1)))
}