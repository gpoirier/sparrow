package com.mediative.sparrow.poc

import com.mediative.sparrow.Alias._

import scalaz._
import scalaz.syntax.validation._

trait DataSchema { self =>

  type Row
  type FieldDescriptor

  type Self = this.type

  def fieldNames: IndexedSeq[String]

  def findField(name: String): FieldDescriptor

  def is[T: PrimitiveType](field: FieldDescriptor): Boolean
  def get[T: PrimitiveType](field: FieldDescriptor): Safe[T]
}

sealed trait PrimitiveType[T]

object PrimitiveType {
  implicit case object StringType extends PrimitiveType[String]
  implicit case object LongType extends PrimitiveType[Long]
  implicit case object IntType extends PrimitiveType[Int]
  implicit case object ByteType extends PrimitiveType[Byte]
  implicit case object FloatType extends PrimitiveType[Float]
  implicit case object DoubleType extends PrimitiveType[Double]
  implicit case object BigInt extends PrimitiveType[BigInt]
  implicit case object BigDecimal extends PrimitiveType[BigDecimal]
  implicit case object DateType extends PrimitiveType[java.util.Date]
}
