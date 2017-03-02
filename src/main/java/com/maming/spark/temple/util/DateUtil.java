package com.maming.spark.temple.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	//public static final DateFormat YMD_SINGLE = new SimpleDateFormat("yyyyMMdd");
	//public static final DateFormat YMD_Middler_SINGLE = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateTimeFormatter YMD_Middler_SINGLE = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static final DateTimeFormatter YMD_SINGLE = DateTimeFormat.forPattern("yyyyMMdd");

	public static String aggregatorDateBy(String date, int dayNum) {
        return YMD_SINGLE.print(YMD_SINGLE.parseDateTime(date).plusDays(dayNum));
    }

    public static String aggregatorDateMiddle(String date, int dayNum) {
        return YMD_Middler_SINGLE.print(YMD_Middler_SINGLE.parseDateTime(date).plusDays(dayNum));
    }

	public static void main(String[] args) {
		String date = "20141016";
		String before30Day = DateUtil.aggregatorDateBy(date, -29);// 计算前30天的日期
		String before7Day = DateUtil.aggregatorDateBy(date, -6);// 计算前7天的日期
		String before3Day = DateUtil.aggregatorDateBy(date, 2);// 计算前3天的日期

		System.out.println(before30Day);
		System.out.println(before7Day);
		System.out.println(before3Day);

	}
}
