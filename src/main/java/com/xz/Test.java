package com.xz;

import com.xz.utils.DateUtil;

import java.io.IOException;
import java.text.ParseException;

/**
 * @author kouyouyang
 * @date 2019-07-24 11:34
 */
public class Test {
    public static void main(String[] args) throws IOException, ParseException {
//        String s = InetAddress.getLocalHost().getHostAddress().replace(".", "-");
//        System.out.println(s);
        String timestamp= DateUtil.stampToBeijingDate("1565150399669", 8);

        System.out.println(timestamp);

    }




}
