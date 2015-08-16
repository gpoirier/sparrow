package com.mediative.sparrow.poc4
package spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.{Transformer => _, _}

import scala.reflect.ClassTag
import scalaz._
import scalaz.syntax.validation._

import RowConverter._
import com.mediative.sparrow.Alias._

object DataFrameReaderTest {
  case class Simple(name: String, count: Long)

  object Simple {
    implicit val schema = (
      field[String]("name") and
      field[Long]("count")
    )(apply, unapply)
  }

  case class WithSimpleOption(name: String, count: Long, description: Option[String])

  object WithSimpleOption {
    implicit val schema = (
      field[String]("name") and
      field[Long]("count") and
      field[Option[String]]("description")
    )(apply, unapply)
  }

  case class WithNested(name: String, inner: Simple, innerOpt: Option[WithSimpleOption])

  object WithNested {
    implicit val schema = (
      field[String]("name") and
      field[Simple]("inner") and
      field[Option[WithSimpleOption]]("innerOpt")
    )(apply, unapply)
  }

  case class SimpleMap(name: String, count: Int)

  object SimpleMap {
    implicit val schema = (
      field[String]("name") and
      field[String]("count").transform(Transformer.from[String, Int](_.toInt, _.toString))
    )(apply, unapply)
  }

  sealed abstract class PetType
  case object Dog extends PetType
  case object Cat extends PetType
  case object Hamster extends PetType

  object PetType {
    implicit val schema: FieldConverter[PetType] =
      FieldConverter(Transformer.from[String, PetType](
        reader = {
          case "dog" => Dog
          case "cat" => Cat
          case "hamster" => Hamster
        },
        writer = {
          case Dog => "dog"
          case Cat => "cat"
          case Hamster => "hamster"
        }
      )
    )
  }

  case class Pet(name: String, tpe: PetType)

  object Pet {
    implicit val schema = (
      field[String]("name") and
      field[PetType]("type")
    )(apply, unapply)
  }
}

class DataFrameReaderTest extends FreeSpec with BeforeAndAfterAll {

  import DataFrameReaderTest._

  val sc = new SparkContext("local", "test2")

  override def afterAll() = sc.stop()

  "RowConverter" - {

    def testSerialization(obj: Any) = {
      import java.io._
      val buf = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(buf)
      out.writeObject(obj)
      out.flush()

      val in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray))
      assert(obj.getClass == in.readObject().getClass)
    }

    "can be serialized" in {
      testSerialization(Simple.schema.reader)
      testSerialization(Simple.schema.writer)
      testSerialization(WithNested.schema.reader)
      testSerialization(WithNested.schema.writer)
    }
  }

  "toRDD" - {

    import DataFrameReader._

    def testSuccess[T: RowConverter: ClassTag](json: Array[String], expected: List[T]) = {
      val sqlContext = new SQLContext(sc)

      val rows = sqlContext.jsonRDD(sc.parallelize(json))
      def testFromRow() = {
        val rdd = toRDD[T](rows).valueOr { es => fail((es.head :: es.tail).mkString("\n")) }

        assert(rdd.collect().toList == expected)
      }
      testFromRow()

      def testToRow() = {
        val df = toDataFrame(sc.parallelize(expected), sqlContext)

        def compareTypes(tpe1: StructType, tpe2: StructType): Unit = {
          for (i1 <- tpe1.indices) {
            val f1 = tpe1.fields(i1)
            val fieldName = f1.name
            val i2 = tpe2.fieldNames.indexOf(fieldName)
            if (i2 == -1) {
              assert(f1.nullable, s"A required field is missing: $f1.")
            } else {
              val f2 = tpe2.fields(i2)
              (f1.dataType, f2.dataType) match {
                case (tpe11: StructType, tpe22: StructType) => compareTypes(tpe11, tpe22)
                case (dt1, dt2) => assert(dt1 == dt2, s"field name: $f1.name")
              }
            }
          }
        }
        compareTypes(df.schema, rows.schema)

        def compareRows(r1: Row, r2: Row): Unit = {
          for (i1 <- r1.schema.indices) {
            val v1 = r1.get(i1)
            val fieldName = r1.schema.fieldNames(i1)
            val i2 = r2.schema.fieldNames.indexOf(fieldName)
            val v2 = if (i2 == -1) null else r2.get(i2)
            (v1, v2) match {
              case (r11: Row, r22: Row) => compareRows(r11, r22)
              case _ => assert(v1 == v2, s"r1: $r1, r2: $r2")
            }
          }
        }

        (df.collect().toList zip rows.collect().toList) foreach { case (r1, r2) =>
          compareRows(r1, r2)
        }
      }
      testToRow()
    }

    def testFailure[T: RowConverter: ClassTag](json: Array[String], expected: NonEmptyList[String]) = {
      val sqlContext = new SQLContext(sc)
      val df = sqlContext.jsonRDD(sc.parallelize(json))

      assert(toRDD[T](df) == expected.failure)
    }

    "work for simple case class with only primitives" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": "Last's inner", "count": 12}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("Last's inner", count = 12)
      )

      testSuccess(json, expected)
    }

    "support optional fields" - {
      "when completely missing from the json" in {
        val json = Array(
          """{"name": "First's name", "count": 121}""",
          """{"name": "Last's name", "count": 12}"""
        )
        val expected = List(
          WithSimpleOption("First's name", count = 121, None),
          WithSimpleOption("Last's name", count = 12, None)
        )

        testSuccess(json, expected)
      }
      "when partially present in the json" in {
        val json = Array(
          """{"name": "First's name", "count": 121, "description": "abc"}""",
          """{"name": "Last's name", "count": 12}"""
        )
        val expected = List(
          WithSimpleOption("First's name", count = 121, Some("abc")),
          WithSimpleOption("Last's name", count = 12, None)
        )

        testSuccess(json, expected)
      }
    }

    "supported nested objects" in {
      val json = Array(
        """{"name": "Guillaume", "inner": {"name": "First Inner", "count": 121}}""",
        """{"name": "Last", "inner": {"name": "Last Inner", "count": 12}}"""
      )
      val expected = List(
        WithNested("Guillaume", Simple("First Inner", 121), None),
        WithNested("Last", Simple("Last Inner", 12), None)
      )

      testSuccess(json, expected)
    }

    "validate extra fields" in {
      val json = Array(
        """{"name": "Guillaume", "inner": {"name": "First's Inner", "count": 121, "abc": 244}}""",
        """{"name": "Last", "inner": {"name": "Last's inner", "count": 12}}"""
      )

      testFailure[WithNested](json, NonEmptyList.nel("There are extra fields: Set(abc)", Nil))
    }

    "validate mixed type for a field with conversion possible (e.g. same colum has both String and Int)" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": 2, "count": 12}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("2", count = 12)
      )

      testSuccess(json, expected)
    }

    "validate mixed type for a field without conversion possible (e.g. same colum has both String and Int)" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": "Second", "count": "12"}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("Second", count = 12)
      )

      testFailure[Simple](json, NonEmptyList.nel(
        "The field 'count' isn't a LongType as expected, StringType received.", Nil))
    }

    "work with ADT enums" in {
      val json = Array(
        """{"name": "Chausette", "type": "dog"}""",
        """{"name": "Mixcer", "type": "cat"}"""
      )
      val expected = List(
        Pet("Chausette", Dog),
        Pet("Mixcer", Cat)
      )

      testSuccess(json, expected)
    }
  }
}
