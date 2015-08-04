package com.mediative.sparrow.poc5

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

sealed trait Safe[+T] {
  def map[U](f: T => U): Safe[U] = this match {
    case Proper(value) => Proper(f(value))
    case other: Failed => other
  }
  def flatMap[U](f: T => Safe[U]): Safe[U] = this match {
    case Proper(value) => f(value)
    case other: Failed => other
  }

  def getRequired: T = toOption getOrElse {
    sys.error("Required value is missing")
  }

  def toOption: Option[T] = this match {
    case Proper(value) => Some(value)
    case Invalid(message) => sys.error(message)
    case Missing => None
  }
}

object Safe {
  def apply[T](block: => T): Safe[T] =
    Proper(block) // TODO Use flatMap to have error handling (which needs to be added to flatMap)

  // TODO Make this a real applicative
  def map2[A, B, C](sa: Safe[A], sb: Safe[B])(f: (A, B) => C): Safe[C] =
    for {
      a <- sa
      b <- sb
    } yield f(a, b)
}

sealed trait Failed extends Safe[Nothing]
case class Proper[+T](value: T) extends Safe[T]
case class Invalid(error: String) extends Failed
case object Missing extends Failed


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
  case class DateType(fmt: DateTimeFormatter) extends PrimitiveType[DateTime]
}

trait Context {
  type Row

  sealed trait FieldType[T] extends Serializable

  object FieldType {

    //implicit case object NullType extends FieldType[Nothing]

    implicit case object RowType extends FieldType[Row]

    case class ListType[T](implicit elementType: FieldType[T]) extends FieldType[List[T]]
    implicit def listType[T: FieldType] = ListType[T]

    case class PrimitiveFieldType[T](implicit primitiveType: PrimitiveType[T]) extends FieldType[T]
    implicit def primitiveType[T: PrimitiveType]: FieldType[T] = PrimitiveFieldType[T]
  }

  import FieldType._

//  case class TypedValue[T](fieldType: FieldType[T], value: Safe[T])
//
//  object TypedValue {
//    def apply[T](value: T)(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, Safe(value))
//  }

//  case class Field(name: String, fieldType: FieldType[_], optional: Boolean)
//  case class Schema(fields: IndexedSeq[Field]) {
//    def +(other: Schema) = Schema(fields ++ other.fields)
//  }
//
//  object Schema {
//    def apply(fields: Field*): Schema = Schema(fields.toIndexedSeq)
//  }

}

//sealed trait FieldConverter[A] {
//  def fieldType: FieldType[_]
//  def reader[B]: (FieldType[B], Safe[B] => Safe[A])
//  def writer[B]: (FieldType[B], A => Option[B])
//}
//
//object FieldConverter {
//
//  implicit def stringConverter: FieldConverter[Long] = ???
//  implicit def longConverter: FieldConverter[String] = ???
//
//  implicit def optionConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Option[A]] =
//    new FieldConverter[Option[A]] {
//      override def fieldType: FieldType[_] = fc.fieldType
//      override def reader[B]: (FieldType[B], Safe[B] => Safe[Option[A]]) = ???
//      override def writer[B]: (FieldType[B], Option[A] => Option[B]) = ???
//    }
//
//  implicit def safeConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Safe[A]] =
//    new FieldConverter[Safe[A]] {
//      override def fieldType: FieldType[_] = fc.fieldType
//      override def reader[B]: (FieldType[B], Safe[B] => Safe[Safe[A]]) = ???
//      override def writer[B]: (FieldType[B], Safe[A] => Option[B]) = ???
//    }
//
//  implicit def fromRowConverter[A](implicit rc: RowConverter[A]): FieldConverter[A] =
//    ???
//}
