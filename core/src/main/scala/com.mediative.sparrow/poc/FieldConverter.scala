package com.mediative.sparrow.poc

import com.mediative.sparrow.Alias._

import scalaz.Success
import scalaz.syntax.apply._
import scalaz.syntax.validation._

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

trait FieldConverter[T] extends Serializable { self =>

  def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[dc.Row => Unsafe[T]]

  def map[U](f: T => U): FieldConverter[U] = mapUnsafe(_.map(f))

  def mapUnsafe[U](f: Unsafe[T] => Unsafe[U]): FieldConverter[U] = new FieldConverter[U] {
    override def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[dc.Row => Unsafe[U]] =
      self.apply(dc)(field).map { _ andThen f }
  }

}

object FieldConverter {

  def apply[A: FieldConverter, B](f: A => B): FieldConverter[B] = reader[A].map(f)
  def unsafe[A: FieldConverter, B](f: Unsafe[A] => Unsafe[B]): FieldConverter[B] = reader[A].mapUnsafe(f)

  def reader[T](implicit fc: FieldConverter[T]): FieldConverter[T] = fc

  def primitiveConverter[T: PrimitiveType](typeName: String) = new FieldConverter[T] {
    override def apply(dc: DataSchema)(field: dc.FieldDescriptor): V[(dc.Row) => Unsafe[T]] = {
      if (!dc.is[T](field)) {
        s"The field $field is expected to be of type $typeName".failureNel
      } else Success { row =>
        dc.get[T](field)
      }
    }
  }

  implicit val stringConverter = primitiveConverter[String]("String")
  implicit val intConverter = primitiveConverter[Int]("Int")
  implicit val longConverter = primitiveConverter[Long]("Long")
  implicit val byteConverter = primitiveConverter[Byte]("Byte")
  implicit val floatConverter = primitiveConverter[Float]("Float")
  implicit val doubleConverter = primitiveConverter[Double]("Doube")
  implicit val bigDecimalConverter = primitiveConverter[BigDecimal]("BigDecimal")
  implicit val bigIntConverter = primitiveConverter[BigInt]("BigInt")

  // TODO USE MAP UNSAFE
  implicit def localDateConverter: FieldConverter[LocalDate] = FieldConverter(LocalDate.parse)
  implicit def dateTimeConverter: FieldConverter[DateTime] = FieldConverter(DateTime.parse)

  //  import java.sql.Timestamp
  //  implicit def timestampConverter: FieldConverter[Timestamp] = longConverter.map(new Timestamp(_))
  //
  //
   //  implicit def optionConverter[T](implicit fc: FieldConverter[T]): FieldConverter[Option[T]] =
  //    new FieldConverter[Option[T]] {
  //      override def isNullable: Boolean = true
  //      override def apply(struct: NamedStruct): V[Row => Option[T]] = {
  //        import struct.index
  //        if (index == -1) Success(row => None)
  //        else fc(struct) map { f => row => Some(row).filterNot(_.isNullAt(index)).map(f) }
  //      }
  //    }
  //
  //  implicit def fieldConverter[T](implicit rc: RowConverter[T]): FieldConverter[T] =
  //    new FieldConverter[T] {
  //      override def apply(struct: NamedStruct): V[Row => T] = {
  //        import struct.index
  //        val dt = struct.field.dataType
  //        dt match {
  //          case tpe: StructType =>
  //            rc.validateAndApply(tpe) map { f =>
  //              row =>
  //                struct.nullCheck(row)
  //                f(row.getAs[Row](index))
  //            }
  //          case _ => "StructType expected, received: $dt".failureNel
  //        }
  //      }
  //    }
}

