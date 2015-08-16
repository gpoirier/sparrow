package com.mediative.sparrow.poc4

import org.scalatest._

import RowConverter._
import ConverterTester._
import FieldType._
import Field.Implicits._
import FieldDescriptor.Implicits._


class RowConverterSpec extends FreeSpec {

  "RowConverter" - {
    "should support list of objects" in {

      case class Simple(name: String, count: Int)
      object Simple {
        implicit val schema = (
          field[String]("name") and
            field[Int]("count")
          )(apply, unapply)

        implicit val tpe: HasSchema[Simple] = Schema(
          "name" -> StringType,
          "count" -> IntType
        )
      }

      case class Parent(id: Long, children: List[Simple])
      object Parent {
        implicit val schema = (
          field[Long]("id") and
            field[List[Simple]]("children")
          )(apply, unapply)

        implicit val tpe: HasSchema[Parent] = Schema(
          "id" -> LongType,
          "children" -> ListType(RowType(Simple.tpe()))
        )
      }

      val parent = Parent(3L, List(
        Simple("First", 4),
        Simple("Second", 114),
        Simple("Third", 42)
      ))

      implicit val listType = ListType(RowType(Simple.tpe()))
      val row = Sparrow(
        "id" -> 3L,
        "children" -> List(
          Sparrow("name" -> "First", "count" -> 4),
          Sparrow("name" -> "Second", "count" -> 114),
          Sparrow("name" -> "Third", "count" -> 42)
        )
      )

      test(row, parent)
    }

    "shoud suppport list of primitives" in {
      implicit val listType = ListType(StringType)

      case class Primitives(name: String, values: List[String])
      object Primitives {
        implicit val schema = (
          field[String]("name") and
          field[List[String]]("values")
        )(apply, unapply)

        implicit val tpe: HasSchema[Primitives] = Schema(
          "name" -> StringType,
          "values" -> listType
        )
      }
      val p = Primitives("Name", List("value #1", "value #2"))

      val row = Sparrow(
        "name" -> "Name",
        "values" -> List("value #1", "value #2")
      )
      test(row, p)
    }
  }
}
