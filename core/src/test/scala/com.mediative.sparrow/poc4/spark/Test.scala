package com.mediative.sparrow.poc4
package spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._
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

//  "RowConverter" - {
//
//    def testSerialization(obj: Any) = {
//      import java.io._
//      val buf = new ByteArrayOutputStream()
//      val out = new ObjectOutputStream(buf)
//      out.writeObject(obj)
//      out.flush()
//
//      val in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray))
//      assert(obj.getClass == in.readObject().getClass)
//    }
//
//    "can be serialized" in {
//      val simple = StructType(Seq(StructField("name", StringType), StructField("count", LongType)))
//      val withNested = StructType(Seq(
//        StructField("name", StringType),
//        StructField("inner", simple),
//        StructField("innerOpt", simple, nullable = true)))
//
//      Simple.schema.validateAndApply(simple) match {
//        case Success(f) =>
//          testSerialization(f)
//          testSerialization(f(Row("Name", 12L)))
//        case Failure(e) => fail(e.toString)
//      }
//
//      WithNested.schema.validateAndApply(withNested) match {
//        case Success(f) =>
//          testSerialization(f)
//          testSerialization(f(Row("Name", Row("Name", 12L), null)))
//        case Failure(e) => fail(e.toString)
//      }
//    }
//  }

  "toRDD" - {

    import DataFrameReader._

    def testSuccess[T: RowConverter: ClassTag](json: Array[String], expected: List[T]) = {
      val sqlContext = new SQLContext(sc)

      val rows = sqlContext.jsonRDD(sc.parallelize(json))
      def fromRow() = {
        val rdd = toRDD[T](rows).valueOr { es => fail((es.head :: es.tail).mkString("\n")) }

        assert(rdd.collect().toList == expected)
      }
      fromRow()

//      def toRow() = {
//        val df = toDataFrame(sc.parallelize(expected), sqlContext)
//
//        assert(df.collectAsList() == rows.collectAsList())
//        assert(df.schema == rows.schema)
//      }
//      toRow()
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
////
//    "validate extra fields" in {
//      val json = Array(
//        """{"name": "Guillaume", "inner": {"name": "First's Inner", "count": 121, "abc": 244}}""",
//        """{"name": "Last", "inner": {"name": "Last's inner", "count": 12}}"""
//      )
//
//      testFailure[WithNested](json, NonEmptyList.nel("There are extra fields: Set(abc)", Nil))
//    }
//
//    "validate mixed type for a field with conversion possible (e.g. same colum has both String and Int)" in {
//      val json = Array(
//        """{"name": "First's Inner", "count": 121}""",
//        """{"name": 2, "count": 12}"""
//      )
//      val expected = List(
//        Simple("First's Inner", count = 121),
//        Simple("2", count = 12)
//      )
//
//      testSuccess(json, expected)
//    }
//
//    "validate mixed type for a field without conversion possible (e.g. same colum has both String and Int)" in {
//      val json = Array(
//        """{"name": "First's Inner", "count": 121}""",
//        """{"name": "Second", "count": "12"}"""
//      )
//      val expected = List(
//        Simple("First's Inner", count = 121),
//        Simple("Second", count = 12)
//      )
//
//      testFailure[Simple](json, NonEmptyList.nel(
//        "The field 'count' isn't a LongType as expected, StringType received.", Nil))
//    }

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
