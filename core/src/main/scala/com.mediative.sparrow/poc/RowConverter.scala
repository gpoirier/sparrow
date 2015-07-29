package com.mediative.sparrow.poc

import scalaz._
import scalaz.syntax.apply._
import scalaz.syntax.validation._

import com.mediative.sparrow.Alias._

import play.api.libs.functional.{ Applicative => PApplicative, Functor => PFunctor, FunctionalBuilderOps }

trait RowConverter[T] extends Serializable { self =>

  def apply(dc: DataSchema): V[dc.Row => Safe[T]]

  def mapSafe[U](f: Safe[T] => Safe[U]): RowConverter[U] = new RowConverter[U] {
    override def apply(dc: DataSchema): V[dc.Row => Safe[U]] = {
      for {
        g <- self(dc)
      } yield {
        g andThen f
      }
    }
  }

  def map[U](f: T => U): RowConverter[U] = mapSafe(_.map(f))


  def toRow(src: T): org.apache.spark.sql.Row = ???
}

object RowConverter {

  object syntax {
    import play.api.libs.functional.syntax.functionalCanBuildApplicative

    implicit def toFunctionalBuilderOps[A](a: RowConverter[A]): FunctionalBuilderOps[RowConverter, A] = {
      val fcb = functionalCanBuildApplicative(RowConverterApplicative)
      play.api.libs.functional.syntax.toFunctionalBuilderOps(a)(fcb)
    }
  }

  implicit object RowConverterApplicative extends PApplicative[RowConverter] with PFunctor[RowConverter] {
    def pure[A](a: A): RowConverter[A] = new RowConverter[A] {
      override def apply(dc: DataSchema) = Success(_ => Safe(a))
    }

    def fmap[A, B](m: RowConverter[A], f: A => B): RowConverter[B] = map(m, f)
    def map[A, B](m: RowConverter[A], f: A => B): RowConverter[B] = m.map(f)

    def apply[A, B](mf: RowConverter[A => B], ma: RowConverter[A]): RowConverter[B] = new RowConverter[B] {
      override def apply(dc: DataSchema): V[dc.Row => Safe[B]] = {
        (ma(dc) |@| mf(dc)) { (rsa, rsab) =>
          // TODO Use \@| instead of a Monad
          (row: dc.Row) => for {
            ab <- rsab(row)
            a <- rsa(row)
          } yield ab(a)
        }
      }
    }
  }

  def field[T](name: String)(implicit fc: FieldConverter[T]): RowConverter[T] =
    new RowConverter[T] {
      override def apply(dc: DataSchema): V[dc.Row => Safe[T]] = {
        // TODO Restore schema validationrr
        fc(dc)(dc.findField(name))
      }
    }
}

import RowConverter._
import RowConverter.syntax._

case class Simple(name: String, count: Long)

object Simple {
  implicit val schema = (
    field[String]("name") and
    field[Long]("count")
  )(apply _)
}
