package com.rmzx.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  *
  * 用于格式化时间字段的工具类
  */
class DateFormatHour extends Serializable {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH")
  val dateFormat3 = new SimpleDateFormat("yyyyMMddHH")
  val dateFormat4 = new SimpleDateFormat("yyyy-MM-dd")
  val dateFormat5 = new SimpleDateFormat("yyyyMMdd")
  val dateFormat6 = new SimpleDateFormat("d")
  val dateFormat7 = new SimpleDateFormat("yyyy-MM")
  val dateFormat8 = new SimpleDateFormat("yyyyMM")
  val dateFormat9 = new SimpleDateFormat("yyyy")
  val dateFormat10 = new SimpleDateFormat("MM/dd")
  val dateFormat11 = new SimpleDateFormat("yyyy/MM")
  val dateFormat12 = new SimpleDateFormat("MM月dd日")
  val dateFormat13 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val dateFormat14 = new SimpleDateFormat("HH")
  val dateFormat15 = new SimpleDateFormat("yyyyMMddHHmmss")
  val dateFormat16 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  val dateFormat17 = new SimpleDateFormat("MM")

  val calendar = Calendar.getInstance()
  val calendar2 = Calendar.getInstance()


  def isValidDate(time:String):Boolean = try {
    val time1 = dateFormat.parse(time).getTime
    true
  } catch {
    case e: Exception =>
      false
  }


  //计算两个"yyyyMM"日期之间差几个月
  def monthDistince (month1:String,month2:String):Int = {
    calendar.setTime(dateFormat8.parse(month1))
    calendar2.setTime(dateFormat8.parse(month2))
    val result = calendar.get(Calendar.MONTH) - calendar2.get(Calendar.MONTH)
    val month = (calendar.get(Calendar.YEAR) - calendar2.get(Calendar.YEAR)) * 12
    Math.abs(month + result)
  }
  //将version号的时间戳转换成前一个小时的时间戳
  def getCertainDayTime(time: String, amount: Int): String = {
    val format1: String = dateFormat2.format(time.toLong)
    calendar.setTime(dateFormat2.parse(format1))
    calendar.add(Calendar.HOUR, amount)
    val time1 = calendar.getTimeInMillis + ""
    calendar.add(Calendar.HOUR, -amount)
    time1.trim
  }

  //将时间戳转换成一年前的时间戳
  def getCertainYearTime(time: String, amount: Int): String = {
    val format1: String = dateFormat2.format(time.toLong)
    calendar.setTime(dateFormat2.parse(format1))
    calendar.add(Calendar.YEAR, amount)
    val time1 = calendar.getTimeInMillis + ""
    calendar.add(Calendar.YEAR, -amount)
    time1.trim
  }

  //将时间戳转换为几周前的时间戳
  def getCertainWeekTime(time: String, amount: Int): String = {
    val format1: String = dateFormat2.format(time.toLong)
    calendar.setTime(dateFormat2.parse(format1))
    calendar.add(Calendar.WEEK_OF_YEAR, amount)
    val time1 = calendar.getTimeInMillis + ""
    calendar.add(Calendar.WEEK_OF_YEAR, -amount)
    time1.trim
  }

  //将时间戳转换为几月前的时间戳
  def getCertainMonthTime(time: String, amount: Int): String = {
    val format1: String = dateFormat7.format(time.toLong)
    calendar.setTime(dateFormat7.parse(format1))
    calendar.add(Calendar.MONTH, amount)
    val time1 = calendar.getTimeInMillis + ""
    calendar.add(Calendar.MONTH, -amount)
    time1.trim
  }

  //将时间戳转换为最近几个月的时间戳
  def getCertainMonthTime2(time: String, amount: Int): String = {
    val format1: String = dateFormat4.format(time.toLong)
    calendar.setTime(dateFormat4.parse(format1))
    calendar.add(Calendar.MONTH, amount)
    val time1 = calendar.getTimeInMillis + ""
    calendar.add(Calendar.MONTH, -amount)
    time1.trim
  }


  //输入yyyyMM ，返回月份对应的天数

  def getMonthDay(time: String): Int = {

    calendar.setTime(dateFormat8.parse(time))
    val maximum = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    maximum
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回周几
  def getWeek(time: String): Int = {
    calendar.setTime(dateFormat.parse(time))
    val a: Int = calendar.get(Calendar.DAY_OF_WEEK)
    a
  }

  //输入输入yyyy-MM-dd'T'HH:mm:ss， 返回周几
  def getWeek2(time: String): Int = {
    calendar.setTime(dateFormat13.parse(time))
    val a: Int = calendar.get(Calendar.DAY_OF_WEEK)
    a
  }

  //输入输入yyyyMMddHHmmss， 返回周几
  def getWeek3(time: String): Int = {
    calendar.setTime(dateFormat15.parse(time))
    val a: Int = calendar.get(Calendar.DAY_OF_WEEK)
    a
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回时间戳
  def getTime(time: String): Long = {
    val time1 = dateFormat.parse(time).getTime
    time1
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回时间戳
  def getTime2(time: String): Long = {
    val time1 = dateFormat13.parse(time).getTime
    time1
  }

  //输入yyyyMMddHHmmss， 返回时间戳
  def getTime4(time: String): Long = {
    val time1 = dateFormat15.parse(time).getTime
    time1
  }

  //输入yyyy-MM-dd， 返回时间戳
  def getTime3(time: String): Long = {
    val time1 = dateFormat4.parse(time).getTime
    time1
  }
  //输入yyyyMM， 返回时间戳
  def getTime5(time: String): Long = {
    val time1 = dateFormat8.parse(time).getTime
    time1
  }

  //输入yyyyMMdd， 返回时间戳
  def getTime6(time: String): Long = {
    val time1 = dateFormat5.parse(time).getTime
    time1
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回HH
  def getDay2(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat14.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回HH
  def getDay3(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat14.format(time1)
    chart
  }

  //输入yyyyMMddHHmmss， 返回HH
  def getDay4(time: String): String = {
    val time1 = dateFormat15.parse(time).getTime
    val chart = dateFormat14.format(time1)
    chart
  }


  //输入yyyy-MM-dd HH:mm:ss， 返回yyyy
  def getYear4(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat9.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyy
  def getYear5(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat9.format(time1)
    chart
  }



  //输入yyyy-MM-dd HH:mm:ss， 返回yyyy-MM-dd HH
  def getHour(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat2.format(time1)
    chart
  }

  //输入yyyy-MM-dd HH  ，返回yyyyMMddHH
  def getHour2(time: String): String = {
    val time1 = dateFormat2.parse(time).getTime
    val chart = dateFormat3.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss  ，返回yyyyMMddHH
  def getHour7(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat3.format(time1)
    chart
  }


  //输入yyyy-MM-dd HH:mm:ss  ，返回yyyyMMdd
  def getHour5(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat5.format(time1)
    chart
  }


  //输入yyyy-MM-dd HH:mm:ss  ，返回yyyy-MM-dd
  def getHour6(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat4.format(time1)
    chart
  }

  //输入yyyy-MM-dd HH:mm:ss  ，返回yyyy/MM/dd HH:mm:ss
  def getFormat(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat16.format(time1)
    chart
  }

  //输入yyyyMMddHHmmss， 返回yyyy/MM/dd HH:mm:ss
  def getFormat2(time: String): String = {
    val time1 = dateFormat15.parse(time).getTime
    val chart = dateFormat16.format(time1)
    chart
  }
  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyy-MM-dd HH
  def getHour3(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat2.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyy-MM-dd
  def getChart5(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat4.format(time1)
    chart
  }


  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyyMMdd
  def getChart10(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat5.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyyMMddHHmmss
  def getChart8(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat15.format(time1)
    chart
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回yyyyMMddHHmmss
  def getChart11(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat15.format(time1)
    chart
  }

  //输入yyyyMMddHHmmss， 返回yyyy-MM-dd
  def getChart6(time: String): String = {
    val time1 = dateFormat15.parse(time).getTime
    val chart = dateFormat4.format(time1)
    chart
  }

  //输入yyyyMMddHHmmss， 返回yyyy-MM-dd'T'HH:mm:ss
  def getChart7(time: String): String = {
    val time1 = dateFormat15.parse(time).getTime
    val chart = dateFormat13.format(time1)
    chart
  }
  //输入yyyyMMddHHmmss， 返回yyyy-MM-dd HH:mm:ss
  def getChart9(time: String): String = {
    val time1 = dateFormat15.parse(time).getTime
    val chart = dateFormat.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyy-MM
  def getMonth5(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat7.format(time1)
    chart
  }

  //输入yyyy-MM-dd'T'HH:mm:ss， 返回yyyyMM
  def getMonth7(time: String): String = {
    val time1 = dateFormat13.parse(time).getTime
    val chart = dateFormat8.format(time1)
    chart
  }

  //输入yyyyMM， 返回MM
  def getMonth8(time: String): String = {
    val time1 = dateFormat8.parse(time).getTime
    val chart = dateFormat17.format(time1)
    chart
  }


  //穿入时间戳，返回yyyy-MM-dd
  def getDay(time: String): String = {
    val day = dateFormat4.format(time.toLong)
    day
  }

  //穿入时间戳，返回yyyyMMddHHmmss
  def getDay5(time: String): String = {
    val day = dateFormat15.format(time.toLong)
    day
  }


  //穿入时间戳，返回yyyy-MM-dd HH
  def getHour4(time: String): String = {
    val hour = dateFormat2.format(time.toLong)
    hour
  }

  //传入yyyy-MM-dd HH ，返回yyyy-MM-dd
  def getChart(time: String): String = {
    val time1 = dateFormat2.parse(time).getTime
    val chart = dateFormat4.format(time1)
    chart
  }

  //传入yyyy-MM-dd ，返回MM/dd
  def getChart2(time: String): String = {
    val time1 = dateFormat4.parse(time).getTime
    val chart = dateFormat10.format(time1)
    chart
  }

  //传入yyyy-MM-dd ，返回MM月dd日
  def getChart3(time: String): String = {
    val time1 = dateFormat4.parse(time).getTime
    val chart = dateFormat12.format(time1)
    chart
  }

  //传入yyyyMMdd，返回yyyy-MM-dd
  def getChart4(time: String): String = {
    val time1 = dateFormat5.parse(time).getTime
    val chart = dateFormat4.format(time1)
    chart
  }

  //传入yyyy-MM-dd HH ，返回yyyy-MM
  def getMonth(time: String): String = {
    val time1 = dateFormat2.parse(time).getTime
    val chart = dateFormat7.format(time1)
    chart
  }

  //穿入时间戳，返回yyyy-MM
  def getMonth2(time: String): String = {
    val day = dateFormat7.format(time.toLong)
    day
  }

  //传入yyyy-MM ，返回yyyy/MM
  def getMonth3(time: String): String = {
    val time1 = dateFormat7.parse(time).getTime
    val identi = dateFormat11.format(time1)
    identi
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回yyyy-MM
  def getMonth4(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat7.format(time1)
    chart
  }

  //输入yyyy-MM-dd HH:mm:ss， 返回yyyyMM
  def getMonth6(time: String): String = {
    val time1 = dateFormat.parse(time).getTime
    val chart = dateFormat8.format(time1)
    chart
  }

  //传入yyyy-MM-dd HH ，返回yyyy
  def getYear(time: String): String = {
    val time1 = dateFormat2.parse(time).getTime
    val year = dateFormat9.format(time1)
    year
  }

  //穿入时间戳，返回yyyy
  def getYear2(time: String): String = {
    val day = dateFormat9.format(time.toLong)
    day
  }

  //传入yyyy-MM-dd ，返回yyyy
  def getYear3(time: String): String = {
    val time1 = dateFormat4.parse(time).getTime
    val year = dateFormat9.format(time1)
    year
  }


  //传入yyyy-MM-dd ，返回yyyyMMdd
  def getIdenti(time: String): String = {
    val time1 = dateFormat4.parse(time).getTime
    val identi = dateFormat5.format(time1)
    identi
  }

  //传入yyyy-MM ，返回yyyyMM
  def getMonthIdenti(time: String): String = {
    val time1 = dateFormat7.parse(time).getTime
    val identi = dateFormat8.format(time1)
    identi
  }

  //传入yyyy-MM-dd ，返回d
  def getDaystr(time: String): String = {
    val time1 = dateFormat4.parse(time).getTime
    val identi = dateFormat6.format(time1)
    identi
  }

}
