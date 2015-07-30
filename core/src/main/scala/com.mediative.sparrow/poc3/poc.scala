package com.mediative.sparrow.poc3

import scala.reflect.ClassTag

import scalaz._
import scalaz.syntax.apply._
import scalaz.syntax.validation._

import play.api.libs.functional.{
  Applicative => PApplicative,
  Functor => PFunctor,
  InvariantFunctor,
  FunctionalBuilderOps
}

import com.mediative.sparrow.Alias._

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

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


trait Context {
  type Row

  sealed trait FieldType[T] extends Serializable

  object FieldType {

    implicit case object NullType extends FieldType[Nothing]

    implicit case object RowType extends FieldType[Row]

    case class ListType[T](implicit elementType: FieldType[T]) extends FieldType[List[T]]
    implicit def listType[T: FieldType] = ListType[T]

    case class PrimitiveFieldType[T](implicit primitiveType: PrimitiveType[T]) extends FieldType[T]
    implicit def primitiveType[T: PrimitiveType]: FieldType[T] = PrimitiveFieldType[T]
  }

  import FieldType._

  case class TypedValue[T](fieldType: FieldType[T], value: Safe[T])

  object TypedValue {
    def apply[T](value: T)(implicit fieldType: FieldType[T]): TypedValue[T] = TypedValue(fieldType, Safe(value))
  }

  case class Field(name: String, fieldType: FieldType[_], optional: Boolean)
  case class Schema(fields: IndexedSeq[Field]) {
    def +(other: Schema) = Schema(fields ++ other.fields)
  }

  object Schema {
    def apply(fields: Field*): Schema = Schema(fields.toIndexedSeq)
  }

}

trait ReadContext extends Context {
  type FieldDescriptor

  override type Row <: InputRow

  //implicit def rowClassTag: ClassTag[Row]

  def getField(name: String): Option[FieldDescriptor]

  trait InputRow {
    def get(field: FieldDescriptor): TypedValue[_]
  }

}

trait WriteContext extends Context {
  override type Row <: OutputRow

  trait OutputRow {
    def +(field: (String, TypedValue[_])): Row
    def +(row: Row): Row
  }

  def emptyRow: Row
}

trait FieldConverter[T] extends Serializable { self =>
  def primaryType(c: Context): c.FieldType[_]
  def reader(c: ReadContext): V[c.TypedValue[_] => Safe[T]]
  def writer(c: WriteContext): T => c.TypedValue[_]
  def optional: Boolean
  def missing: Safe[T]

  def transform[U](transformer: Transformer[T, U]): FieldConverter[U] = new FieldConverter[U] {
    override def writer(c: WriteContext): U => c.TypedValue[_] =
      self.writer(c) compose transformer.writer

    override def reader(c: ReadContext): V[c.TypedValue[_] => Safe[U]] =
      self.reader(c).map(_ andThen transformer.reader)

    override def primaryType(c: Context): c.FieldType[_] = self.primaryType(c)
    override def optional: Boolean = self.optional
    override def missing = transformer.reader(self.missing)
  }
}

object FieldConverter {
  def converter[T](implicit fc: FieldConverter[T]): FieldConverter[T] = fc
  def apply[T: FieldConverter, U](transformer: Transformer[T, U]): FieldConverter[U] =
    converter[T].transform(transformer)

  def primitive[T: PrimitiveType: ClassTag] = new FieldConverter[T] {
    override def optional: Boolean = false
    override def missing = Missing
    override def primaryType(c: Context): c.FieldType[_] = c.FieldType.primitiveType

    override def reader(c: ReadContext): V[(c.TypedValue[_]) => Safe[T]] = {
      val Type = c.FieldType.primitiveType
      Success {
        case c.TypedValue(Type, value) => value.asInstanceOf[Safe[T]]
        case c.TypedValue(tpe, value) => Invalid(s"Unexpected field type ($tpe) for value: $value, expecting $Type")
      }
    }
    override def writer(c: WriteContext): T => c.TypedValue[_] =
      value => c.TypedValue(c.FieldType.primitiveType, Safe(value))
  }

  implicit val stringConverter: FieldConverter[String] = primitive
  implicit val longConverter: FieldConverter[Long] = primitive
  implicit val intConverter: FieldConverter[Int] = primitive
  implicit val byteConverter: FieldConverter[Byte] = primitive
  implicit val floatConverter: FieldConverter[Float] = primitive
  implicit val doubleConverter: FieldConverter[Double] = primitive
  implicit val bigIntConverter: FieldConverter[BigInt] = primitive
  implicit val bigDecimalConverter: FieldConverter[BigDecimal] = primitive

  val isoDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  implicit def dateTimeConverter(fmt: DateTimeFormatter): FieldConverter[DateTime] =
    primitive(PrimitiveType.DateType(fmt), implicitly[ClassTag[DateTime]])
  implicit val isoDateTimeConverter: FieldConverter[DateTime] = dateTimeConverter(isoDateFormat)

  val dateTimeToLocalDate = Transformer.from[DateTime, LocalDate](
    reader = _.toLocalDate,
    writer = _.toDateTimeAtStartOfDay
  )

  implicit def isoLocalDateConverter: FieldConverter[LocalDate] = FieldConverter(dateTimeToLocalDate)

  implicit def localDateConverter(fmt: DateTimeFormatter): FieldConverter[LocalDate] =
    dateTimeConverter(fmt).transform(dateTimeToLocalDate)
  implicit def dateTimeConverterFromString(pattern: String): FieldConverter[DateTime] =
    DateTimeFormat.forPattern(pattern)
  implicit def localDateConverterFromString(pattern: String): FieldConverter[LocalDate] =
    DateTimeFormat.forPattern(pattern)

  implicit def optionConverter[T](implicit fc: FieldConverter[T]): FieldConverter[Option[T]] =
    new FieldConverter[Option[T]] {
      override def primaryType(c: Context): c.FieldType[_] = fc.primaryType(c)
      override def optional: Boolean = true
      override def missing = Proper(None)

      override def reader(c: ReadContext): V[c.TypedValue[_] => Safe[Option[T]]] =
        fc.reader(c).map { f =>
          {
            case c.TypedValue(_, Missing) => Proper(None)
            case other => f(other) match {
              case Proper(v) => Proper(Some(v))
              case Missing => Proper(None)
              case failure: Invalid => failure
            }
          }
        }

      override def writer(c: WriteContext): Option[T] => c.TypedValue[_] =
        _.map(fc.writer(c)).getOrElse(c.TypedValue(primaryType(c), Missing))

    }

  // This type is only meant to be used for reading uncleaned data
  // its write will not save failures, they'll be discarded.
  implicit def safeConverter[T](implicit fc: FieldConverter[T]): FieldConverter[Safe[T]] =
    new FieldConverter[Safe[T]] {
      override def primaryType(c: Context): c.FieldType[_] = fc.primaryType(c)
      override def optional: Boolean = true
      override def missing = Proper(Missing)

      override def reader(c: ReadContext): V[c.TypedValue[_] => Safe[Safe[T]]] =
        fc.reader(c).map { f => f(_).map(Proper.apply) }
      override def writer(c: WriteContext): Safe[T] => c.TypedValue[_] = {
        val write = fc.writer(c);
        {
          case Proper(value) => write(value)
          // TODO Use non-Proper type when it exists
          case Missing => c.TypedValue(primaryType(c), Missing)
          // Error values will not be persisted
          case error: Invalid => c.TypedValue(primaryType(c), error)
        }
      }
    }

  implicit def fromRowConverter[T](implicit rc: RowConverter[T]): FieldConverter[T] = new FieldConverter[T] {

    override def primaryType(c: Context): c.FieldType[_] = c.FieldType.RowType
    override def optional: Boolean = false
    override def missing = Missing

    override def reader(c: ReadContext): V[c.TypedValue[_] => Safe[T]] =
      rc.reader(c).map { f =>
        import c.FieldType._
        {
          case c.TypedValue(RowType, safeRow) => safeRow.flatMap(f)
          case c.TypedValue(tpe, value) => sys.error(s"Unexpected type ($tpe) for value $value. Row expected.")
        }
      }
    override def writer(c: WriteContext): T => c.TypedValue[_] = {
      val write = rc.writer(c)
      value => c.TypedValue(c.FieldType.RowType, Safe(write(value)))
    }
  }
}

case class Transformer[A, B](reader: Safe[A] => Safe[B], writer: B => A)

object Transformer {
  def from[A, B](reader: A => B, writer: B => A): Transformer[A, B] =
    Transformer(_.map(reader), writer)
}

trait RowConverter[T] extends Serializable { self =>
  def schema(c: Context): c.Schema
  def reader(c: ReadContext): V[c.Row => Safe[T]]
  def writer(c: WriteContext): T => c.Row

  def transform[U](transformer: Transformer[T, U]): RowConverter[U] = new RowConverter[U] {
    override def reader(c: ReadContext): V[c.Row => Safe[U]] =
      self.reader(c).map(_ andThen transformer.reader)
    override def writer(c: WriteContext): U => c.Row =
      transformer.writer andThen self.writer(c)
    override def schema(c: Context): c.Schema =
      self.schema(c)
  }

  def and[U](that: RowConverter[U]): RowConverter[(T, U)] =
    new RowConverter[(T, U)] {
      override def schema(c: Context): c.Schema = self.schema(c) + that.schema(c)

      override def reader(c: ReadContext): V[c.Row => Safe[(T, U)]] = {
        (self.reader(c) |@| that.reader(c)) { (rsa, rsb) => row =>
          val sa = rsa(row)
          val sb = rsb(row)
          Safe.map2(sa, sb)((_, _))
        }
      }
      override def writer(ctx: WriteContext): ((T, U)) => ctx.Row = {
        val fa = self.writer(ctx)
        val fb = that.writer(ctx);
        { case (a, b) => fa(a) + fb(b) }
      }
    }

}

object RowConverter {
  def field[T](name: String)(implicit fc: FieldConverter[T]): RowConverter[T] = new RowConverter[T] {
    override def schema(c: Context): c.Schema = {
      import c._
      Schema(Field(name, fc.primaryType(c), fc.optional))
    }
    override def reader(c: ReadContext): V[c.Row => Safe[T]] = {
      import Validation.FlatMap._
      fc.reader(c).flatMap { reader =>
        c.getField(name).map { field =>
          Success { row: c.Row =>
            reader(row.get(field))
          }
        } getOrElse {
          if (fc.optional) Success { row: c.Row => fc.missing }
          else s"The field $name is missing.".failureNel
        }
      }
    }
    override def writer(c: WriteContext): T => c.Row = {
      val w = fc.writer(c)
      src => c.emptyRow + (name -> w(src))
    }
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

case class Simple(name: String, count: Long)

object Simple {
  implicit val schema = (
    field[String]("name") and
    field[Long]("count")
  )((name: String, count: Long) => apply(name, count), unapply)
}

case class Parent(name: String, child: Option[Simple])

object Parent {
  implicit val schema = (
    field[String]("name") and
    field[Option[Simple]]("child")
  )(apply, unapply)
}
