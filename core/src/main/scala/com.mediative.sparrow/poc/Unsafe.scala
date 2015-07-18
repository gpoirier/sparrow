package com.mediative.sparrow.poc

sealed trait Unsafe[+T] {
  def map[U](f: T => U): Unsafe[U] = this match {
    case Proper(value) => Proper(f(value))
    case other: Failed => other
  }
  def flatMap[U](f: T => Unsafe[U]): Unsafe[U] = this match {
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

sealed trait Failed extends Unsafe[Nothing]
case class Proper[+T](value: T) extends Unsafe[T]
case class Invalid(error: String) extends Failed
case object Missing extends Failed
