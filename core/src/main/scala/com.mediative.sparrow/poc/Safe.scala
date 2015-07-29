package com.mediative.sparrow.poc

import scala.util.control.{NonFatal, ControlThrowable}

sealed trait Safe[+T] {
  def map[U](f: T => U): Safe[U] = this match {
    case Proper(value) => Proper(f(value))
    case other: Missing => other
  }
  def flatMap[U](f: T => Safe[U]): Safe[U] = this match {
    case Proper(value) =>
      try f(value)
      catch {
        case NonFatal(ex) => Invalid(ex)
      }
    case other: Missing => other
  }

  def getRequired: T = toOption getOrElse {
    sys.error("Required value is missing")
  }

  def toOption: Option[T] = this match {
    case Proper(value) => Some(value)
    case Invalid(ex) => throw ex
    case Empty => None
  }
}

case class BadFormat(message: String)
  extends Exception(message)
  with ControlThrowable

sealed trait Missing extends Safe[Nothing]

case object Empty extends Missing
case class Invalid(error: Throwable) extends Missing
case class Proper[+T](value: T) extends Safe[T]

object Safe {

  def empty[T]: Safe[T] = Empty

  def apply[T](unsafe: => T): Safe[T] =
    Safe.fromOption(Some(unsafe))

  def fromOption[T](unsafe: => Option[T]): Safe[T] =
    Proper(()).flatMap(_ => unsafe.fold(empty[T])(Proper.apply))
}