package com.xz.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/**
 * @author kouyouyang
 * @date 2019-07-21 23:32
 */
public class DateUtil {

    //获取当天时间
    public static String getToday2seconds() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }

    /**
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }


    /**
     * 将时间戳转换为北京时间带毫秒
     * timeZoneOffset原为int类型，为班加罗尔调整成float类型
     * timeZoneOffset表示时区，如中国一般使用东八区，因此timeZoneOffset就是8
     * @return
     */
    public static String stampToBeijingDate(String s,float timeZoneOffset){
        if (timeZoneOffset > 13 || timeZoneOffset < -12) {
            timeZoneOffset = 0;
        }
        int newTime=(int)(timeZoneOffset * 60 * 60 * 1000);
        TimeZone timeZone;
        String[] ids = TimeZone.getAvailableIDs(newTime);
        if (ids.length == 0) {
            timeZone = TimeZone.getDefault();
        } else {
            timeZone = new SimpleTimeZone(newTime, ids[0]);
        }
        String res;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(timeZone);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = sdf.format(date);
        return res;
    }


    public static String getDayFromTimeStamp(String timestamp){
        String[] str1 = timestamp.split(" ");
        return str1[0];
    }

    public static String getHourFromTimeStamp(String timestamp){
        String[] str1 = timestamp.split(" ");
        String[] str2 = str1[1].split(":");
        return str2[0];
    }

    public static String getMinuteFromTimeStamp(String timestamp){
        String[] str1 = timestamp.split(" ");
        String[] str2 = str1[1].split(":");
        return str2[1];
    }


    public static String getSecondFromTimeStamp(String timestamp){
        String[] str1 = timestamp.split(" ");
        String[] str2 = str1[1].split(":");
        String[] str3 = str2[2].split("\\.");
        return str3[0];
    }

    public static String getMillionsFromTimeStamp(String timestamp){
        String[] str1 = timestamp.split(" ");
        String[] str2 = str1[1].split(":");
        //注意，必须转义
        String[] str3 = str2[2].split("\\.");
        return str3[1];
    }


}
