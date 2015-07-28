package com.mediative.sparrow.poc3
package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row => SparkRow, _ }
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

import com.mediative.sparrow.Alias._

case class NamedStruct(name: String, tpe: StructType) {
  def index = tpe.fieldNames.indexOf(name)
  def field = tpe.fields.lift(index) getOrElse {
    sys.error(
      s"Cannot find field '$name' in fields: ${tpe.fields.toList}" +
        s"(field names: ${tpe.fieldNames.toList}, index: $index)")
  }

  def description: String = s"$name ($field)"
}

class SparkReadContext(tpe: StructType) extends ReadContext {
  override type FieldDescriptor = NamedStruct
  override type Row = SparkInputRow

  case class SparkInputRow(underlying: SparkRow, schema: Schema) extends InputRow {
    override def get(field: NamedStruct): Safe[TypedValue[_]] = {
      import field._
      if (underlying.isNullAt(index)) Missing
      else underlying.get(index) match {
        case x: String => Safe(TypedValue(x))
        case x: Int => Safe(TypedValue(x))
        case x: Long => Safe(TypedValue(x))
      }
    }
  }

  override def getField(name: String): Option[FieldDescriptor] = {
    val field = NamedStruct(name, tpe)
    if (field.index == -1) None
    else Some(field)
  }

}

object SparkWriteContext extends WriteContext {

  override type Row = SparkOutputRow

  trait SparkOutputRow extends OutputRow

  override def emptyRow: SparkWriteContext.Row = ???
}

object DataFrameReader {
  def toRDD[T: ClassTag](df: DataFrame)(implicit rc: RowConverter[T]): V[RDD[T]] = {
    //rc.validateAndApply(df.schema).map { f => df.map(f) }
    val c = new SparkReadContext(df.schema)
    val schema = rc.schema(c)
    rc.reader(c).map { f => df.map { row => f(c.SparkInputRow(row, schema)).getRequired } }
  }
}