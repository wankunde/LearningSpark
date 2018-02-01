package json

import org.json4s.jackson.JsonMethods._
/***
  * http://json4s.org/
  */
object Json2CaseClassWithJackson extends App {
  implicit val formats = org.json4s.DefaultFormats

  /* case class Child(name: String, age: Int)
   case class Address(street: String, city: String)
   case class Person(name: String, address: Address, children: List[Child])
   val jsonStr = parse("""
       { "name": "joe",
         "address": {
           "street": "Bulevard",
           "city": "Helsinki"
         },
         "children": [
           {
             "name": "Mary",
             "age": 5
           },
           {
             "name": "Mazy",
             "age": 3
           }
         ]
       }
     """)

   val person = jsonStr.extract[Person]
   println(person)*/

  case class SkuInfo(sku_id: String, sku_action: Integer) // 字段名称要与json的key值相同
  case class IOTPoint(x: Integer, y: Integer, enter_time: Integer, leave_time: Integer, sku_list: List[SkuInfo])
  case class DMPoint(x: Integer, y: Integer, var stay_seconds: Integer, sku_list: List[SkuInfo])
  case class IOTBehaviorTrace(gateway_id: String, face_id: String, trace_id:String, start_time: Integer, end_time: Integer, data: List[IOTPoint])
  case class DMBehaviorTrace(request_id: String, gateway_id: String, face_id: String, trace_id:String, start_time: Integer, end_time: Integer, data: List[DMPoint])
  val s = "{\"gateway_id\":\"44488b839a0b4661a9d77c4ac1de570e\",\"face_id\":\"\",\"trace_id\":\"98fb40e306cd44f7a9f4b5977d1852d7\",\"start_time\":1517274076,\"end_time\":1517274094," +
    "\"data\":[{\"x\":821,\"y\":219,\"enter_time\":1517274076,\"leave_time\":1517274077," +
    "\"sku_list\":[{\"sku_id\":\"af44603207f946d8b34327fc7ea5871f\",\"sku_action\":0}," +
    "{\"sku_id\":\"6051ce69fcb6435f85bc98cd42ba6505\",\"sku_action\":2}," +
    "{\"sku_id\":\"095cceae0a77498d9bfdce20a938daca\",\"sku_action\":2}," +
    "{\"sku_id\":\"18b110a16aa44aefadc0c4d20ec6133f\",\"sku_action\":0}," +
    "{\"sku_id\":\"7a478b19f10b489c9bb3a7b3d14346b0\",\"sku_action\":0}]}]}"
  val iotBehaviorTrace:IOTBehaviorTrace = parse(s).extract[IOTBehaviorTrace]
  println(iotBehaviorTrace)
}