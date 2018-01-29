package collection



case class DMPoint1(x: Integer, y: Integer, var stay_seconds: Integer, sku_id: String, sku_action: String)
case class DM(x: Integer, y: Integer, var stay_seconds: Integer, list: List[Map[String,String]])
object ListTest extends App {
  val site: List[DMPoint1] = List(DMPoint1(2,3,3,"1","3"), DMPoint1(2,3,3,"1","3"),DMPoint1(2,4,3,"1","3"),DMPoint1(2,4,3,"1","3"))
  val groupByXY = site.groupBy(item => (item.x, item.y))
  println(groupByXY)

  val sum = groupByXY.mapValues(_.reduce((a,b) => DMPoint1(a.x, a.y, a.stay_seconds + b.stay_seconds, a.sku_id,a.sku_action)))
  println(sum.values.toList)

/*  val sum = groupByXY.mapValues(_.reduce((a,b) => {
    var map = Map(a.sku_id -> a.sku_action,
                   b.sku_id -> b.sku_action)
    var list = List[Map[String,String]]()
    var dm = DM
    list = list :+ map
    dm.apply(a.x, a.y, a.stay_seconds, list)
    dm
  })*/

//  sum.values.toList
//  println(sum)
}




