package com.mediative.sparrow.poc4

import org.scalatest._

import com.github.nscala_time.time.Imports._

import RowConverter._
import ConverterTester._

import FieldType._
import FieldDescriptor.Implicits._
import Field.Implicits._


class DateTimeOptionsSpec extends FreeSpec {


  case class DateTimeHolder(
    name: String,
    dateTime: DateTime)

  object DateTimeHolder {
    implicit val schema = (
      field[String]("name") and
      field[DateTime]("dateTime")(DatePattern("dd/MM/yyyy HH:mm:ss"))
    )(apply, unapply)

    implicit val tpe: HasSchema[DateTimeHolder] = Schema(
      "name" -> StringType,
      "dateTime" -> DateType("dd/MM/yyyy HH:mm:ss")
    )
  }

  case class LocalDateHolder(
    name: String,
    dateTime: LocalDate)

  object LocalDateHolder {
    implicit val schema = (
      field[String]("name") and
      field[LocalDate]("dateTime")(DatePattern("dd/MM/yyyy"))
    )(apply, unapply)

    implicit val tpe: HasSchema[LocalDateHolder] = Schema(
      "name" -> StringType,
      "dateTime" -> DateType("dd/MM/yyyy")
    )
  }

  "DateTimeRowConverter" - {
    "should allow define a custom date format for DateTime fields" in {
      val row = Sparrow(
        "name" -> "Hello",
        Field("dateTime", TypedValue(DateType("dd/MM/yyyy HH:mm:ss"), Safe(DateTime.parse("2015-12-25T14:40:00.00"))))
      )
      test(row, DateTimeHolder("Hello", DateTime.parse("2015-12-25T14:40:00.00")))
    }
    "should allow define a custom date format for LocalDate fields" in {
      val row = Sparrow(
        "name" -> "Hello",
        Field("dateTime", TypedValue(DateType("dd/MM/yyyy"), Safe(DateTime.parse("2015-12-25T00:00:00.00"))))
      )
      test(row, LocalDateHolder("Hello", LocalDate.parse("2015-12-25")))
    }
  }
}
