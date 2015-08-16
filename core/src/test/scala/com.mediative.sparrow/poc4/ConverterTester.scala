package com.mediative.sparrow.poc4

import org.scalatest.Matchers

object ConverterTester extends Matchers {

  trait HasSchema[T] extends (() => Schema)
  implicit def toTpe[T](tpe: Schema): HasSchema[T] = new HasSchema[T] {
    def apply() = tpe
  }

  def test[T](row: Sparrow, expected: T)(implicit rc: RowConverter[T], tpe: HasSchema[T]) = {
    assert(rc.schema == tpe())
    assert(rc.reader(row) == Safe(expected))
    assert(rc.writer(expected) == row)
  }
}
