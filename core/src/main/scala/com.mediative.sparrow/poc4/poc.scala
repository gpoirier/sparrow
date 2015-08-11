package com.mediative.sparrow.poc4

import com.github.nscala_time.time.Imports._
import com.mediative.sparrow.poc3.PrimitiveType
import com.mediative.sparrow.poc4.FieldType.RowType
import org.joda.time.format.DateTimeFormatter

import scala.reflect.ClassTag

sealed trait Safe[+T] {
  def map[U](f: T => U): Safe[U] = this match {
    case Proper(value) => Proper(f(value))
    case other: Failed => other
  }
  def flatMap[U](f: T => Safe[U]): Safe[U] = this match {
    case Proper(value) => f(value)
    case other: Failed => other
  }

  // FIXME
  def |@|[U](other: Safe[U]): Safe[(T, U)] =
    for {
      t <- this
      u <- other
    } yield (t, u)

  def getRequired: T = toOption getOrElse {
    sys.error("Required value is missing")
  }

  def getOrElse[U >: T](defaultValue: => U): U = this match {
    case Proper(value) => value
    case _ => defaultValue
  }


  def toOption: Option[T] = this match {
    case Proper(value) => Some(value)
    case Invalid(message) => sys.error(message)
    case Missing => None
  }
}

object Safe {
  def apply[T](block: => T): Safe[T] = {
    val value = block
    // TODO Use flatMap to have error handling (which needs to be added to flatMap)
    if (value != null) Proper(value)
    else Missing
  }

  // TODO Make this a real applicative
  def map2[A, B, C](sa: Safe[A], sb: Safe[B])(f: (A, B) => C): Safe[C] =
    for {
      a <- sa
      b <- sb
    } yield f(a, b)

  def fromOption[T](opt: Option[T]): Safe[T] = opt.fold[Safe[T]](Missing)(Proper.apply)
}

sealed trait Failed extends Safe[Nothing]
case class Proper[+T](value: T) extends Safe[T]
case class Invalid(error: String) extends Failed
case object Missing extends Failed

sealed trait FieldType[T] {
  def isCompatible(that: FieldType[_]): Boolean = {
    if (this == that) true
    else isTypeCompatible(this, that)
  }

  import FieldType._
  private def isTypeCompatible(ft1: FieldType[_], ft2: FieldType[_]): Boolean =
    (ft1, ft2) match {
      case (DateType(_), DateType(_)) => true
      case (RowType(_), RowType(_)) => true
      case (ListType(el1), ListType(el2)) => el1 isCompatible el2
      case _ => false
    }

}

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

//  case class OptionType[T](elementType: FieldType[T]) extends FieldType[Option[T]]
//  case class SafeType[T](elementType: FieldType[T]) extends FieldType[Safe[T]]
}

case class FieldDescriptor(name: String, fieldType: FieldType[_], optional: Boolean)

object FieldDescriptor {
  object Implicits {
    implicit def toDescriptor(tuple: (String, FieldType[_])): FieldDescriptor =
      FieldDescriptor(tuple._1, tuple._2, optional = false)

    implicit def toDescriptor(tuple: (String, FieldType[_], Boolean)): FieldDescriptor =
      FieldDescriptor(tuple._1, tuple._2, tuple._3)
  }
}
case class Schema(fields: IndexedSeq[FieldDescriptor]) {
  def +(other: Schema): Schema = Schema(fields ++ other.fields)
}

object Schema {
  def apply(fields: FieldDescriptor*): Schema =
    Schema(fields.toVector)
}

case class TypedValue[T](fieldType: FieldType[T], value: Safe[T])

object TypedValue {
  def apply[T](value: Safe[T])(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, value)
}

case class Field[T](name: String, fieldType: FieldType[T], value: Safe[T]) {
  def typedValue = TypedValue(fieldType, value)
}

object Field {
  def apply[T](name: String, typedValue: TypedValue[T]): Field[T] =
    Field(name, typedValue.fieldType, typedValue.value)
}

trait AllInstances {

  object Sparrow {
    def apply(fields: Field[_]*): Sparrow = Sparrow(fields.toIndexedSeq)
  }
  case class Sparrow(fields: IndexedSeq[Field[_]]) {
    def apply[T](fieldName: String)(implicit fieldType: FieldType[T]): Safe[T] = {
      val opt = fields.find(_.name == fieldName)
      opt match {
        case Some(Field(_, tpe, value)) if tpe isCompatible fieldType => value.asInstanceOf[Safe[T]]
        case None => Missing
        case _ => sys.error(s"$opt - expected $fieldType ")
      }
    }
    def +(other: Sparrow): Sparrow = Sparrow(fields ++ other.fields)
    def +(field: Field[_]): Sparrow = this + Sparrow(field)
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

case class Transformer[A, B](reader: Safe[A] => Safe[B], writer: B => A)

object Transformer {
  def from[A, B](reader: A => B, writer: B => A): Transformer[A, B] =
    Transformer(_.map(reader), writer)
}

sealed trait FieldConverter[A] extends Serializable { self =>

  type PrimitiveType

  def fieldType: FieldType[PrimitiveType]
  def optional: Boolean

  def reader: Safe[PrimitiveType] => Safe[A]
  def writer: A => Safe[PrimitiveType]

  def transform[B](transformer: Transformer[A, B]): FieldConverter[B] =
    new FieldConverter[B] {
      override type PrimitiveType = self.PrimitiveType
      override def fieldType: FieldType[self.PrimitiveType] = self.fieldType
      override def optional = self.optional

      override def reader: Safe[PrimitiveType] => Safe[B] =
        self.reader andThen transformer.reader

      override def writer: B => Safe[PrimitiveType] =
        self.writer compose transformer.writer
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
      override def optional = false
      override def reader: Safe[A] => Safe[A] = identity
      override def writer: A => Safe[A] = Safe(_)
    }

  implicit def optionConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Option[A]] =
    new FieldConverter[Option [A]] {
      override def fieldType: FieldType[PrimitiveType] = fc.fieldType
      override type PrimitiveType = fc.PrimitiveType
      override def optional = true

      override def reader: Safe[PrimitiveType] => Safe[Option[A]] = fc.reader(_) match {
        case Proper(value) => Proper(Some(value))
        case Missing => Proper(None)
        case other: Invalid => other
      }
      override def writer: Option[A] => Safe[PrimitiveType] =
        _.fold[Safe[PrimitiveType]](Missing)(fc.writer)
    }

  implicit def safeConverter[A](implicit fc: FieldConverter[A]): FieldConverter[Safe[A]] =
    new FieldConverter[Safe[A]] {
      override type PrimitiveType = fc.PrimitiveType
      override def fieldType = fc.fieldType
      override def optional = true
      override def reader: Safe[PrimitiveType] => Safe[Safe[A]] = ???
      override def writer: Safe[A] => Safe[PrimitiveType] = ???
    }

  implicit def fromRowConverter[A](implicit rc: RowConverter[A]): FieldConverter[A] =
    new FieldConverter[A] {
      override def fieldType: FieldType[Sparrow] = FieldType.RowType(rc.schema)
      override type PrimitiveType = Sparrow
      override def optional = false

      override def reader: Safe[Sparrow] => Safe[A] = _.flatMap(rc.reader)
      override def writer: A => Safe[Sparrow] = a => Safe(rc.writer(a))
    }
}

trait RowConverter[A] extends Serializable { self =>
  def schema: Schema
  def reader: Sparrow => Safe[A]
  def writer: A => Sparrow

  def transform[B](transformer: Transformer[A, B]): RowConverter[B] =
    new RowConverter[B] {
      override def schema: Schema = self.schema

      override def reader: Sparrow => Safe[B] =
        self.reader andThen transformer.reader
      override def writer: B => Sparrow =
        transformer.writer andThen self.writer
    }

  def and[B](that: RowConverter[B]): RowConverter[(A, B)] =
    new RowConverter[(A, B)] {
      override def schema: Schema = self.schema + that.schema

      override def reader: Sparrow => Safe[(A, B)] = row =>
        self.reader(row) |@| that.reader(row)
      override def writer: ((A, B)) => Sparrow = {
        case (a, b) => self.writer(a) + that.writer(b)
      }
    }

}

object RowConverter {
  def field[A](name: String)(implicit fc: FieldConverter[A]): RowConverter[A] =
    new RowConverter[A] {
      override def schema: Schema = Schema(FieldDescriptor(name, fc.fieldType, fc.optional))

      override def reader: Sparrow => Safe[A] = {
        val tpe = fc.fieldType
        row => fc.reader(row(name)(tpe))
      }
      override def writer: A => Sparrow = {
        val tpe = fc.fieldType
        value => Sparrow(Field(name, tpe, fc.writer(value)))
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
