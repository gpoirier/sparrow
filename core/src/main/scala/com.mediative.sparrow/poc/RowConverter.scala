package com.mediative.sparrow.poc

import scalaz.Success
import scalaz.syntax.apply._
import scalaz.syntax.validation._

import com.mediative.sparrow.Alias._

trait RowConverter[T] extends Serializable { self =>

  def validateFields(fields: Set[String]): (V[Unit], Set[String])

  def apply(dc: DataSchema): V[dc.Row => T]

  def validateSchema(dc: DataSchema): V[Unit] = {
    val (v, others) = validateFields(dc.fieldNames.toSet)
    val extraFields =
      if (others.isEmpty) ().success
      else s"There are extra fields: $others".failureNel

    (v |@| extraFields) { (_, _) => () }
  }

  def map[U](f: T => U): RowConverter[U] = new RowConverter[U] {
    override def validateFields(fields: Set[String]) = self.validateFields(fields)
    override def apply(dc: DataSchema): V[dc.Row => U] = {
      for {
        g <- self(dc)
      } yield {
        g andThen f
      }
    }
  }

  def validateAndApply(dc: DataSchema): V[dc.Row => T] = {
    import scalaz.Validation.FlatMap._
    validateSchema(dc) flatMap { _ =>
      apply(dc)
    }
  }
}
