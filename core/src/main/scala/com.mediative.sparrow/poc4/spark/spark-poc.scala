package com.mediative.sparrow.poc4
package spark

import com.mediative.sparrow.poc4.FieldType.RowType
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

    Success(df.map { row => f(toSparrow(schema, row)).getRequired })
  }

  def toDataFrame[T](rdd: RDD[T], sql: SQLContext)(implicit rc: RowConverter[T]) = {
    val struct: StructType = toStruct(rc.schema)
    val rows = rdd.map(rc.writer andThen toRow)
    sql.createDataFrame(rows, struct)
  }

  def toRow(row: Sparrow): Row = {
    val values = row.fields.map(_.typedValue).map {
      case TypedValue(RowType(_), value) => value.asInstanceOf[Safe[Sparrow]].map(toRow)
      // TODO Handle List
      case TypedValue(_, value) => value
    }
    Row(values.map(_.getOrElse(null)): _*)
  }

  def toStruct(schema: Schema): StructType =
    StructType(schema.fields.map(toField))

  def toField(field: FieldDescriptor): StructField = {
    def go: FieldType[_] => DataType = {
      case FieldType.StringType => StringType
      case FieldType.ByteType => ByteType
      case FieldType.IntType => IntegerType
      case FieldType.LongType => LongType
      case FieldType.FloatType => FloatType
      case FieldType.DoubleType => DoubleType
      case FieldType.BigDecimalType => DecimalType.Unlimited
      case FieldType.BigIntType => DecimalType.Unlimited
      case FieldType.DateType(_) => TimestampType
      case FieldType.RowType(el) => toStruct(el)
      case FieldType.ListType(el) => ArrayType(go(el))
    }
    val dataType = go(field.fieldType)
    StructField(field.name, dataType, field.optional)
  }

  def toSparrow(struct: StructType, row: Row): Sparrow = {
    val fields = for {
      (tpe, index) <- struct.fields.zipWithIndex
    } yield {
      val typedValue = tpe.dataType match {
        case StringType => TypedValue(Safe(row.getString(index)))
        case LongType => TypedValue(Safe(row.getLong(index)))
        case childType: StructType =>
          val childValue = row.getStruct(index)
          val childRow = toSparrow(childType, childValue)
          val fieldType = FieldType.RowType(toSchema(childType))
          TypedValue(fieldType, Safe(childRow))
        case _ => ???
      }
      Field(tpe.name, typedValue)
    }
    Sparrow(fields: _*)
  }

  def toSchema(struct: StructType): Schema = {
    val fields = struct.fields.map { field =>
      val fieldType = field.dataType match {
        case StringType => FieldType.StringType
        case LongType => FieldType.LongType
      }
      FieldDescriptor(field.name, fieldType, field.nullable)
    }
    Schema(fields.toVector)
  }
}

/*
case class RowWrapper0(struct: StructType, row: Row) /*extends Sparrow */ {
  def apply[A](name: String)(implicit fieldType: FieldType[A]): A = {
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
}
*/
