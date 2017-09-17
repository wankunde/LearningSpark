import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Period, PeriodType}

import scala.util.{Failure, Success, Try}

/**
  * Created by admin on 2017/2/24.
  */

object TimeUtils {

  def _toDateTime(dt: String): DateTime = if (dt.contains(" ")) {
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.forID("Etc/GMT-8")).parseDateTime(dt)
  } else {
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.forID("Etc/GMT-8")).parseDateTime(s"$dt 00:00:00")
  }

  def toDateTime(dt: String, format: String = "yyyy-MM-dd HH:mm:ss"): String = _toDateTime(dt).toString(format)

  def toDate(dt: String): String = _toDateTime(dt).toString("yyyy-MM-dd")

  def toDateTry(dt: String): String = Try(toDate(dt)) match {
    case Success(v) => v
    case Failure(e) => ""
  }

  def dtToDate(dt: String): String = if (dt.length == 8) {
    val year = dt.substring(0, 4)
    val month = dt.substring(4, 6)
    val day = dt.substring(6, 8)
    toDate(s"$year-$month-$day")
  } else today

  def toHour(dt: String): String = _toDateTime(dt).getHourOfDay.toString

  def today: String = new DateTime().toString("yyyy-MM-dd")

  def now: String = new DateTime().toString("yyyy-MM-dd HH:mm:ss")

  def someday(n: Int, dt: String = today, format: String = "yyyy-MM-dd"): String =
    _toDateTime(dt).plusDays(n).toString(format)

  def tomorrow(dt: String, format: String = "yyyy-MM-dd"): String = someday(1, dt, format)

  def yesterday(dt: String, format: String = "yyyy-MM-dd"): String = someday(-1, dt, format)

  def weekday(dt: String = today): String = _toDateTime(dt).dayOfWeek().getAsString

  def rangeDate(start: String, end: String, format: String = "yyyy-MM-dd HH:mm:ss"): Seq[String] = {
    val diff = dateDiff(end, start)
    val startDate = _toDateTime(start)
    for (x <- 0 until diff) yield startDate.plusDays(x).toString(format)
  }

  def rangeDateHour(start: String, end: String, dt: String, format: String = "yyyy-MM-dd HH:mm:ss"): Seq[String] = {
    val startDate = _toDateTime(start)
    val boundary = _toDateTime(dt).toString(format)

    val diff = if (startDate.toString("yyyy-MM-dd") == boundary.substring(0, 10)) 1 else dateDiff(end, start)
    val rangeDts = for (x <- 0 until diff) yield startDate.plusDays(x).toString(format)

    val endDate = _toDateTime(end)
    if (boundary < endDate.toString(format) && boundary > startDate.toString(format) && !rangeDts.contains(boundary))
      boundary +: rangeDts
    else rangeDts
  }

  def dateDiff(end: String, start: String): Int = {
    val p: Period = new Period(_toDateTime(toDate(start)), _toDateTime(toDate(end)), PeriodType.days())
    p.getDays
  }

  def rangeHour(start: String, end: String, format: String = "yyyy-MM-dd HH:mm:ss"): Seq[String] = {
    val diff = hourDiff(end, start)
    val startDate = _toDateTime(start)
    for (x <- 0 until diff) yield startDate.plusHours(x).toString(format)
  }

  def hourDiff(end: String, start: String): Int = {
    val p: Period = new Period(_toDateTime(start), _toDateTime(end), PeriodType.hours())
    p.getHours
  }

}
