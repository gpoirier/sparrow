package com.mediative.sparrow.rwpoc

import com.mediative.sparrow.poc.Safe
import com.mediative.sparrow.Alias._

import scalaz._
import scalaz.syntax.validation._

import play.api.libs.functional.{ Applicative => PApplicative, Functor => PFunctor, FunctionalBuilderOps }

trait InputSchema {
  def fieldNames: IndexedSeq[String]
  def field(name: String): InputField
}

trait InputField {
  def is[T: FieldType](row: Row): Boolean
  def get[T: FieldType](row: Row): Safe[T]
}

trait Row

sealed trait FieldType[T]

object FieldType {
  implicit case object StringType extends FieldType[String]
  implicit case object LongType extends FieldType[Long]
  implicit case object IntType extends FieldType[Int]
  implicit case object ByteType extends FieldType[Byte]
  implicit case object FloatType extends FieldType[Float]
  implicit case object DoubleType extends FieldType[Double]
  implicit case object BigInt extends FieldType[BigInt]
  implicit case object BigDecimal extends FieldType[BigDecimal]
  implicit case object DateType extends FieldType[java.util.Date]
  implicit case object RowType extends FieldType[Row]
  abstract class ListType[T, PT[_] <: FieldType[T]] extends FieldType[List[T]]
  implicit def ListType[T, FT[_] <: FieldType[T]] = new ListType[T, FT] { }
}

case class Value[T: FieldType](value: T)

trait FieldConverter[T] {
  def reader(field: InputField): Row => Safe[T]
  val writer: T => Value[T]
}

object FieldConverter {
  implicit def stringConverter: FieldConverter[String] = ???
  implicit def longConverter: FieldConverter[Long] = ???
}
case class FieldDescription(name: String, fieldType: FieldType[_], optional: Boolean)
case class RowSchema(fields: IndexedSeq[FieldDescription])

trait RowConverter[T] {
  def schema: RowSchema
  def reader(schema: InputSchema): Row => Safe[T]
  val writer: T => Row
}

object RowConverter {
  def field[T: FieldConverter](name: String): RowConverter[T] = ???

  object syntax {
    import play.api.libs.functional.syntax.functionalCanBuildApplicative

    implicit def toFunctionalBuilderOps[A](a: RowConverter[A]): FunctionalBuilderOps[RowConverter, A] = {
      val fcb = functionalCanBuildApplicative(RowConverterApplicative)
      play.api.libs.functional.syntax.toFunctionalBuilderOps(a)(fcb)
    }
  }

  trait RowConverterApplicative extends PApplicative[RowConverter] with PFunctor[RowConverter]
  implicit def RowConverterApplicative: RowConverterApplicative = ???

}

import RowConverter._
import RowConverter.syntax._

case class Simple(name: String, count: Long)

object Simple {
  implicit val schema: RowConverter[Simple] = (
    field[String]("name") and
    field[Long]("count")
  )(apply _)
}

