package com.mediative.sparrow.rwpoc.rw

import scala.reflect.ClassTag

import scalaz._
import Scalaz.ToApplyOps
import scalaz.syntax.validation._

//import org.apache.spark.sql._
//import org.apache.spark.sql.types._

import play.api.libs.functional.{ Applicative => PApplicative, Functor => PFunctor, FunctionalBuilderOps }

import com.mediative.sparrow.Alias._
import com.mediative.sparrow.poc.{ Safe, Missing }

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

trait ReadContext {
  type Row <: RowApi
  type FieldDescriptor

  def getField(name: String): Option[FieldDescriptor]

  trait RowApi {
    def get(field: FieldDescriptor): Safe[TypedValue[_]]
  }
}

trait WriteContext {
  type Row <: RowApi

  def empty: Row

  trait RowApi {
    def +(field: (String, Option[TypedValue[_]])): Row
    def +(row: Row): Row
  }
}

//trait Row {
//  def get(fieldName: String): TypedValue[_]
//}

sealed trait FieldType[T]

object FieldType {
  implicit case object StringType extends FieldType[String]
  implicit case object LongType extends FieldType[Long]
  implicit case object IntType extends FieldType[Int]
  implicit case object ByteType extends FieldType[Byte]
  implicit case object FloatType extends FieldType[Float]
  implicit case object DoubleType extends FieldType[Double]
  implicit case object BigIntType extends FieldType[BigInt]
  implicit case object BigDecimalType extends FieldType[BigDecimal]
  implicit case object DateType extends FieldType[DateWrapper]
  //implicit case object RowType extends FieldType[Row]

  case class ListType[T](implicit elementType: FieldType[T]) extends FieldType[List[T]]
  implicit def listType[T: FieldType] = ListType[T]
}

case class DateWrapper(date: DateTime, format: DateTimeFormatter) {
  override def toString: String = format.print(date)
}

case class TypedValue[T](fieldType: FieldType[T], value: T)

object TypedValue {
  def apply[T](value: T)(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, value)
}

case class Field(name: String, fieldType: FieldType[_], optional: Boolean)
case class Schema(fields: IndexedSeq[Field])

object Schema {
  def apply(fields: Field*): Schema = Schema(fields.toIndexedSeq)
}

case class FieldConverter[T](
  primaryType: FieldType[_],
  read: TypedValue[_] => Safe[T],
  write: T => Option[TypedValue[_]],
  optional: Boolean = false
)

object FieldConverter {
  import FieldType._

  private def primitive[T: ClassTag](implicit fieldType: FieldType[T]): FieldConverter[T] = FieldConverter(
    fieldType,
    read = {
      // TODO Add non-strict types support
      case TypedValue(`fieldType`, value: T) => Safe(value)
    },
    write = { value => Some(TypedValue(value)) }
  )
  implicit def stringConverter: FieldConverter[String] = primitive[String]
  implicit def longConverter: FieldConverter[Long] = primitive[Long]
}

trait RowConverter[T] {
  def schema: Schema
  def read(c: ReadContext): V[c.Row => Safe[T]]
  def write(c: WriteContext): T => c.Row
}

object RowConverter {

  object syntax {
    import play.api.libs.functional.syntax.functionalCanBuildApplicative

    implicit def toFunctionalBuilderOps[A](a: RowConverter[A]): FunctionalBuilderOps[RowConverter, A] = {
      val cbf = functionalCanBuildApplicative(RowConverterApplicative)
      play.api.libs.functional.syntax.toFunctionalBuilderOps(a)(cbf)
    }
  }

  trait RowConverterApplicative extends PApplicative[RowConverter] with PFunctor[RowConverter]

  implicit def RowConverterApplicative: RowConverterApplicative = ???

  def field[T](name: String)(implicit fc: FieldConverter[T]): RowConverter[T] = new RowConverter[T] {
    override def schema: Schema = Schema(Field(name, fc.primaryType, fc.optional))
    override def read(c: ReadContext): V[c.Row => Safe[T]] = {
      c.getField(name).map { field =>
        Success { row: c.Row =>
           row.get(field).flatMap(fc.read)
        }
      } getOrElse {
        if (fc.optional) {
          Success { row: c.Row => Missing }
        } else {
          s"The field $name is missing.".failureNel
        }
      }
    }
    override def write(c: WriteContext): T => c.Row = { src =>
      c.empty + (name -> fc.write(src))
    }
  }
}

import RowConverter._
import RowConverter.syntax._

case class Simple(name: String, count: Long)

object Simple {
  implicit val schema = (
    field[String]("name") and
    field[Long]("count")
  )((name: String, count: Long) => apply(name, count))
}
