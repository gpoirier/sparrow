package com.mediative.sparrow.poc4
package spark

import com.mediative.sparrow.poc4.FieldType.RowType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

import com.mediative.sparrow.Alias._

import scalaz.syntax.validation._
import scalaz.syntax.apply._

import org.log4s._

object DataFrameReader {

  private[this] val logger = getLogger

  def toRDD[T: ClassTag](df: DataFrame)(implicit rc: RowConverter[T]): V[RDD[T]] = {
    val schema = df.schema
    val f = rc.reader
    validate(toStruct(rc.schema), schema) map { _ =>
      df.map { row => f(toSparrow(schema, row)).getRequired }
    }
  }

  def toDataFrame[T](rdd: RDD[T], sql: SQLContext)(implicit rc: RowConverter[T]) = {
    val struct: StructType = toStruct(rc.schema)
    val rows = rdd.map(rc.writer andThen toRow)
    sql.createDataFrame(rows, struct)
  }

  def validate(tpe1: StructType, tpe2: StructType): V[Unit] = {
    logger.trace(s"Comparing two types,\nType 1: $tpe1\nType 2: $tpe2")
    val ok: V[Unit] = ().success
    val (result, remainder) = tpe1.indices.foldLeft(ok -> tpe2.fields.toSet) { case ((acc, fields), i1) =>
      val f1 = tpe1.fields(i1)
      val fieldName = f1.name
      val field2 = fields.find(_.name == fieldName)
      val result =
        field2.fold {
          if (f1.nullable) { logger.trace(s"$f1 has no match but is nullable"); ok }
          else s"A required field is missing: $f1.".failureNel
        } { f2 =>
          (f1.dataType, f2.dataType) match {
            case (tpe11: StructType, tpe22: StructType) => validate(tpe11, tpe22)
            case (dt1, dt2) =>
              if (dt1 == dt2) { logger.trace(s"Field ${f1.name} has matching type $dt1"); ok }
              else s"The field '${f1.name}' isn't a $dt1 as expected, $dt2 received.".failureNel
          }
      }
      (acc |@| result) { (_, _) => () } -> (fields -- field2.toSet)
    }
    if (remainder.isEmpty) result
    else s"There are extra fields: ${remainder.map(_.name)}".failureNel
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
