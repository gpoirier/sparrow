package com.mediative.sparrow.poc4

import com.github.nscala_time.time.Imports._
import com.mediative.sparrow.poc3.PrimitiveType
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

  case class DateType(fmt: DateTimeFormatter) extends FieldType[DateTime]
  case class RowType(schema: Schema) extends FieldType[Sparrow]
  case class ListType[T](elementType: FieldType[T]) extends FieldType[List[T]]

  case class OptionType[T](elementType: FieldType[T]) extends FieldType[Option[T]]
  case class SafeType[T](elementType: FieldType[T]) extends FieldType[Safe[T]]
}

case class FieldDescriptor(name: String, fieldType: FieldType[_])

object FieldDescriptor {
  object Implicits {
    implicit def toDescriptor(tuple: (String, FieldType[_])): FieldDescriptor =
      FieldDescriptor(tuple._1, tuple._2)
  }
}
case class Schema(fields: IndexedSeq[FieldDescriptor])

object Schema {
  def apply(fields: FieldDescriptor*): Schema =
    Schema(fields.toVector)
}

case class TypedValue[T](fieldType: FieldType[T], value: Safe[T])

object TypedValue {
  def apply[T](value: T)(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, Safe(value))
}

case class Field[T](name: String, fieldType: FieldType[T], value: T)

trait AllInstances {

  trait Sparrow {
    def apply[T: FieldType](fieldName: String): T

    def +(other: Sparrow): Sparrow
    def +(field: Field[_]): Sparrow
  }

  import FieldType._
  import FieldDescriptor.Implicits._
  val nested = Schema(
    "id" -> LongType,
    "name" -> StringType,
    "child" -> RowType(
      Schema()
    )
  )
}

case class Transformer[A, B](reader: A => B, writer: B => A)

object Transformer {
  def from[A, B](reader: A => B, writer: B => A): Transformer[A, B] =
    //Transformer(_.map(reader), writer)
    Transformer(reader, writer)
}

sealed trait FieldConverter[A] extends Serializable { self =>

  type PrimitiveType

  def fieldType: FieldType[PrimitiveType]

  def reader: PrimitiveType => A
  def writer(emptyRow: Sparrow): A => PrimitiveType

  def transform[B](transformer: Transformer[A, B]): FieldConverter[B] =
    new FieldConverter[B] {
      override type PrimitiveType = self.PrimitiveType
      override def fieldType: FieldType[self.PrimitiveType] = self.fieldType

      override def reader: PrimitiveType => B =
        self.reader andThen transformer.reader

      override def writer(emptyRow: Sparrow): B => PrimitiveType =
        self.writer(emptyRow) compose transformer.writer
    }
}

object FieldConverter {

//  def converter[A](implicit fc: FieldConverter[A]): FieldConverter[A] = fc

  def apply[A, B](transformer: Transformer[A, B])(implicit fc: FieldConverter[A]): FieldConverter[B] =
    fc.transform(transformer)

  implicit def primitive[A](implicit ft: FieldType[A]): FieldConverter[A] =
    new FieldConverter[A] {
      override type PrimitiveType = A
      override def fieldType: FieldType[PrimitiveType] = ft
      override def reader: A => A = identity
      override def writer(emptyRow: Sparrow): A => A = identity
    }

  implicit def optionConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Option[A]] =
    new FieldConverter[Option [A]] {
      override def fieldType: FieldType[PrimitiveType] = FieldType.OptionType(fc.fieldType)
      override type PrimitiveType = Option[fc.PrimitiveType]

      override def reader: PrimitiveType => Option[A] = _.map(fc.reader)
      override def writer(emptyRow: Sparrow): Option[A] => PrimitiveType = _.map(fc.writer(emptyRow))
    }

  implicit def safeConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Safe[A]] =
    new FieldConverter[Safe[A]] {
      override def fieldType: FieldType[PrimitiveType] = FieldType.SafeType(fc.fieldType)
      override type PrimitiveType = Safe[fc.PrimitiveType]
      override def writer(emptyRow: Sparrow): Safe[A] => PrimitiveType = ???
      override def reader: PrimitiveType => Safe[A] = ???
    }

  implicit def fromRowConverter[A](implicit rc: RowConverter[A]): FieldConverter[A] =
    new FieldConverter[A] {
      override def fieldType: FieldType[Sparrow] = FieldType.RowType(rc.schema)
      override type PrimitiveType = Sparrow

      override def reader: Sparrow => A = rc.reader
      override def writer(emptyRow: Sparrow): A => Sparrow = rc.writer(emptyRow)
    }
}

trait RowConverter[A] extends Serializable { self =>
  def schema: Schema
  def reader: Sparrow => A
  def writer(emptyRow: Sparrow): A => Sparrow

  def transform[B](transformer: Transformer[A, B]): RowConverter[B] =
    new RowConverter[B] {
      override def schema: Schema = self.schema

      override def reader: Sparrow => B =
        self.reader andThen transformer.reader
      override def writer(emptyRow: Sparrow): B => Sparrow =
        transformer.writer andThen self.writer(emptyRow)
    }

  def and[B](that: RowConverter[B]): RowConverter[(A, B)] =
    new RowConverter[(A, B)] {
      override def schema: Schema = self.schema

      override def reader: Sparrow => (A, B) = row =>
        (self.reader(row), that.reader(row))
      override def writer(emptyRow: Sparrow): ((A, B)) => Sparrow = {
        case (a, b) => self.writer(emptyRow)(a) + that.writer(emptyRow)(b)
      }
    }

}

object RowConverter {
  def field[A](name: String)(implicit fc: FieldConverter[A]): RowConverter[A] =
    new RowConverter[A] {
      override def schema: Schema = Schema(FieldDescriptor(name, fc.fieldType))

      override def reader: Sparrow => A = {
        val tpe = fc.fieldType
        row => fc.reader(row(name)(tpe))
      }
      override def writer(emptyRow: Sparrow): A => Sparrow = {
        val tpe = fc.fieldType
        value => emptyRow + Field(name, tpe, fc.writer(emptyRow)(value))
      }
    }

  implicit class RowConverter1[A](val rc: RowConverter[A]) extends AnyVal {
    def apply[B](reader: A => B, writer: B => Option[A]): RowConverter[B] =
      rc.transform(Transformer.from(reader, c => writer(c).get))
  }

  implicit class RowConverter2[A, B](val rc: RowConverter[(A, B)]) extends AnyVal {
    def apply[C](reader: (A, B) => C, writer: C => Option[(A, B)]): RowConverter[C] =
      rc.transform(Transformer.from(reader.tupled, c => writer(c).get))
  }

  implicit class RowConverter3[A, B, C](val rc: RowConverter[((A, B), C)]) extends AnyVal {
    def apply[D](reader: (A, B, C) => D, writer: D => Option[(A, B, C)]): RowConverter[D] =
      rc.transform(Transformer.from(
        { case ((a, b), c) => reader(a, b, c) },
        d => writer(d).get match { case (a, b, c) => ((a, b), c) }
      ))
  }

}


import RowConverter._

case class Single(name: String)

object Single {
  implicit val schema =
    field[String]("name")
      .apply(apply, unapply)
}

case class Simple(name: String, count: Long)

object Simple {
  implicit val schema = (
    field[String]("name") and
    field[Long]("count")
  )((name: String, count: Long) => apply(name, count), unapply)
}

case class Parent(name: String, child: Option[Simple], description: Option[String])

object Parent {
  implicit val schema = (
    field[String]("name") and
    field[Option[Simple]]("child") and
    field[Option[String]]("description")
  )(apply, unapply)
}
