package json

import com.alibaba.fastjson.JSON
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object JsonString2Object extends App {
  val json =new String("{\"gateway_id\":\"1\"}")
  val jsonOjbect = JSON.parseObject(json)
  println()
}
