package collection
case class DMPoint1(x: Integer, y: Integer, var stay_seconds: Integer, sku_id: String, sku_action: String)
object ListTest extends App {
  val site: List[DMPoint1] = List(DMPoint1(2,3,3,"1","3"), DMPoint1(2,3,3,"1","3"),DMPoint1(2,4,3,"1","3"),DMPoint1(2,4,3,"1","3"))
  val groupByXY = site.groupBy(item => (item.x,item.y))
  println(groupByXY)

  val sum = groupByXY.mapValues(_.reduce((a,b) => new DMPoint1(a.x, a.y, a.stay_seconds + b.stay_seconds, a.sku_id, a.sku_action)))
  println(sum)
}




