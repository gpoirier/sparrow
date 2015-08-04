package com.mediative.sparrow.poc4
package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

import com.mediative.sparrow.Alias._

import scalaz.Success

object DataFrameReader {
  def toRDD[T: ClassTag](df: DataFrame)(implicit rc: RowConverter[T]): V[RDD[T]] = {
    val schema = df.schema
    val f = rc.reader

    Success(df.map { row => f(RowWrapper(schema, row)) })
  }
}

case class RowWrapper(struct: StructType, row: Row) extends Sparrow {
  override def apply[A](name: String)(implicit fieldType: FieldType[A]): A = {
    val index = struct.fieldNames.indexOf(name)
    if (index == -1) fieldType match {
      case FieldType.OptionType(_) => None
      case FieldType.SafeType(_) => Missing
      case _ => sys.error("TODO: Message")
    } else {
      def go[T](tpe: FieldType[T]): T = tpe match {
        case FieldType.StringType => row.getString(index)
        case FieldType.LongType => row.getLong(index)
        case FieldType.OptionType(el) =>
          if (row.isNullAt(index)) None
          else Some(go(el))
        case FieldType.SafeType(el) =>
          if (row.isNullAt(index)) Missing
          else Safe(go(el))
        case FieldType.RowType(schema) =>
          struct.fields(index).dataType match {
            case nested: StructType =>
              RowWrapper(nested, row.getStruct(index))
            case _ => sys.error("Expected Row")
          }
        case _ => sys.error("Type Not Implemented yet: " + fieldType)
      }
      go(fieldType)
    }
  }

  override def +(other: Sparrow): Sparrow = ???
  override def +(field: Field[_]): Sparrow = ???
}