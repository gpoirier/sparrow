package com.mediative.sparrow.rwpoc.rw

import scala.reflect.ClassTag

import scalaz._
import Scalaz.ToApplyOps
import scalaz.syntax.validation._

//import org.apache.spark.sql._
//import org.apache.spark.sql.types._

import play.api.libs.functional.{ Applicative => PApplicative, Functor => PFunctor, FunctionalBuilderOps }

import com.mediative.sparrow.Alias._
import com.mediative.sparrow.poc.{ Safe, Missing, Invalid }

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

case class DateWrapper(date: DateTime, format: DateTimeFormatter) {
  override def toString: String = format.print(date)
}

sealed trait PrimitiveType[T]

object PrimitiveType {
  implicit case object StringType extends PrimitiveType[String]
  implicit case object LongType extends PrimitiveType[Long]
  implicit case object IntType extends PrimitiveType[Int]
  implicit case object ByteType extends PrimitiveType[Byte]
  implicit case object FloatType extends PrimitiveType[Float]
  implicit case object DoubleType extends PrimitiveType[Double]
  implicit case object BigIntType extends PrimitiveType[BigInt]
  implicit case object BigDecimalType extends PrimitiveType[BigDecimal]
  implicit case object DateType extends PrimitiveType[DateWrapper]
}

trait Context {
  type Row <: RowApi
  type FieldDescriptor

  implicit def rowClassTag: ClassTag[Row]

  def getField(name: String): Option[FieldDescriptor]

  trait RowApi {
    def get(field: FieldDescriptor): Safe[TypedValue[_]]
    def +(field: (String, Option[TypedValue[_]])): Row
    def +(row: Row): Row
  }

  def emptyRow: Row

  sealed trait FieldType[T]

  object FieldType {
    implicit case object RowType extends FieldType[Row]

    case class ListType[T](implicit elementType: FieldType[T]) extends FieldType[List[T]]
    implicit def listType[T: FieldType] = ListType[T]

    case class PrimitiveFieldType[T](implicit primitiveType: PrimitiveType[T]) extends FieldType[T]
    implicit def primitiveType[T: PrimitiveType] = PrimitiveFieldType[T]
  }

  import FieldType._

  case class TypedValue[T](fieldType: FieldType[T], value: T)

  object TypedValue {
    def apply[T](value: T)(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, value)
  }

  case class Field(name: String, fieldType: FieldType[_], optional: Boolean)
  case class Schema(fields: IndexedSeq[Field])

  object Schema {
    def apply(fields: Field*): Schema = Schema(fields.toIndexedSeq)
  }

  case class FieldConverterApi[T](
    primaryType: FieldType[_],
    reader: V[TypedValue[_] => Safe[T]],
    writer: T => Option[TypedValue[_]],
    optional: Boolean = false
  )

  case class RowConverterApi[T](
    schema: Schema,
    reader: V[Row => Safe[T]],
    writer: T => Row
  )

  def field[T](name: String, fc: FieldConverterApi[T]): RowConverterApi[T] = RowConverterApi[T](
    schema = Schema(Field(name, fc.primaryType, fc.optional)),
    reader = {
      import Validation.FlatMap._

      fc.reader.flatMap { reader =>
        getField(name).map { field =>
          Success { row: Row =>
            row.get(field).flatMap(reader)
          }
        } getOrElse {
          if (fc.optional) {
            Success { row: Row => Missing }
          } else {
            s"The field $name is missing.".failureNel
          }
        }
      }
    },
    writer = src => emptyRow + (name -> fc.writer(src))
  )

  def fromRowConverter[T](rc: RowConverterApi[T]): FieldConverterApi[T] = FieldConverterApi(
    primaryType = RowType,
    reader = rc.reader.map { f =>
      {
        case TypedValue(RowType, row: Row) => f(row)
      }
    },
    writer = { value => Some(TypedValue(RowType, rc.writer(value))) }
  )

  def primitive[T: ClassTag](fieldType: FieldType[T]): FieldConverterApi[T] = FieldConverterApi(
    primaryType = fieldType,
    reader = {
      Success {
        case TypedValue(`fieldType`, value: T) => Safe(value)
        case TypedValue(tpe, value) => Invalid(s"Unexpected field type ($tpe) for value: $value")
      }
    },
    writer = { value => Some(TypedValue(fieldType, value)) }
  )
}

trait FieldConverter[T] {
  def converter(c: Context): c.FieldConverterApi[T]
}

object FieldConverter {

  def primitive[T: PrimitiveType: ClassTag] = new FieldConverter[T] {
    override def converter(c: Context): c.FieldConverterApi[T] = c.primitive(c.FieldType.primitiveType[T])
  }
  implicit def stringConverter: FieldConverter[String] = primitive[String]
  implicit def longConverter: FieldConverter[Long] = primitive[Long]

  implicit def fromRowConverter[T](implicit rc: RowConverter[T]): FieldConverter[T] = new FieldConverter[T] {
    override def converter(c: Context): c.FieldConverterApi[T] = c.fromRowConverter(rc.converter(c))
  }
}

trait RowConverter[T] {
  def converter(c: Context): c.RowConverterApi[T]
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
    override def converter(c: Context): c.RowConverterApi[T] = c.field[T](name, fc.converter(c))
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

case class Parent(name: String, child: Simple)

object Parent {
  implicit val schema = (
    field[String]("name") and
    field[Simple]("child")
  )(apply _)
}
