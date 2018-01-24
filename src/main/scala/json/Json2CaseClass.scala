package json

import org.json4s.jackson.JsonMethods._

/***
  * http://json4s.org/
  */
object Json2CaseClass extends App {
  case class Child(name: String, age: Int)
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
  implicit val formats = org.json4s.DefaultFormats
  val person = jsonStr.extract[Person]
  println(person)
}