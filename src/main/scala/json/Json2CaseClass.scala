package json

import org.json4s.native.Serialization.{read, write}

object Json2CaseClass extends App {
  implicit val formats = org.json4s.DefaultFormats

  case class Class(_name:String, students: List[Student])
  case class Student(sid:String, _name:String)

  val s = "{\"_name\":\"Class1\",\"students\":[{\"sid\":\"1\",\"_name\":\"小明\"},{\"sid\":\"1\",\"_name\":\"小王\"}]}"

  val clazz:Class = read[Class](s)
  println(clazz)

  println(write(clazz))
}