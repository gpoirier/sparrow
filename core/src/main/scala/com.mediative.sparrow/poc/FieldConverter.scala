package com.mediative.sparrow.poc

import com.mediative.sparrow.Alias._

import scala.util.control.NonFatal
import scalaz.Success
import scalaz.syntax.apply._
import scalaz.syntax.validation._

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

sealed trait FieldConverter[T] extends Serializable { self =>

  def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[dc.Row => Safe[T]]

  def map[U](f: T => U): FieldConverter[U] = transform(_.map(f))

  def transform[U](f: Safe[T] => Safe[U]): FieldConverter[U] = new FieldConverter[U] {
    override def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[dc.Row => Safe[U]] =
      self.apply(dc)(field).map {
        _ andThen {
          _.flatMap(x => f(Safe(x)))
        }
      }
  }

  def recoverWith[U >: T](pf: PartialFunction[Safe[T], Safe[U]]): FieldConverter[U] =
    transform(pf.applyOrElse(_, identity[Safe[U]]))
}

object FieldConverter {

  def apply[A: FieldConverter, B](f: A => B): FieldConverter[B] = reader[A].map(f)
  def transform[A: FieldConverter, B](f: Safe[A] => Safe[B]): FieldConverter[B] = reader[A].transform(f)

  def reader[T](implicit fc: FieldConverter[T]): FieldConverter[T] = fc

  def primitiveConverter[T: PrimitiveType](typeName: String) = new FieldConverter[T] {
    override def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[(dc.Row) => Safe[T]] = {
      if (!dc.is[T](field)) {
        s"The field $field is expected to be of type $typeName".failureNel
      } else Success { row =>
        dc.get[T](field, row)
      }
    }
  }

  implicit def safeConverter[T: FieldConverter]: FieldConverter[Safe[T]] = reader[T].transform(Proper.apply)
  implicit def optionConverter[T: FieldConverter]: FieldConverter[Option[T]] = reader[T] transform  {
    case Proper(value) => Proper(Some(value))
    case Empty => Proper(None)
    case error: Invalid => error
  }

  implicit val stringConverter = primitiveConverter[String]("String")
  implicit val intConverter = primitiveConverter[Int]("Int")
  implicit val longConverter = primitiveConverter[Long]("Long")
  implicit val byteConverter = primitiveConverter[Byte]("Byte")
  implicit val floatConverter = primitiveConverter[Float]("Float")
  implicit val doubleConverter = primitiveConverter[Double]("Doube")
  implicit val bigDecimalConverter = primitiveConverter[BigDecimal]("BigDecimal")
  implicit val bigIntConverter = primitiveConverter[BigInt]("BigInt")

  implicit def localDateConverter: FieldConverter[LocalDate] = FieldConverter(LocalDate.parse)
  implicit def dateTimeConverter: FieldConverter[DateTime] = FieldConverter(DateTime.parse)

  //  import java.sql.Timestamp
  //  implicit def timestampConverter: FieldConverter[Timestamp] = longConverter.map(new Timestamp(_))
}

