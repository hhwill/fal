package com.gingkoo.imas.hsbc.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EtlUtils {

    public static int differentDaysByMillisecond(String date1, String date2)
    {
        try {
            Date ddate1 = new SimpleDateFormat("yyyyMMdd").parse(date1);
            Date ddate2 = new SimpleDateFormat("yyyyMMdd").parse(date2);
            int days = (int) ((ddate2.getTime() - ddate1.getTime()) / (1000 * 3600 * 24));
            return Math.abs(days);
        } catch (Exception ex) {
            return 0;
        }
    }

    public static String formatNBJGH(String src) {
        String result = src;
        while (result.length() < 3) {
            result = "0" + result;
        }
        if (result.length() == 3) {
            result = "CNHSBC"+ result;
        }
        return result;
    }

    public static String formatKHH(String src) {
        String result = src;
        if (result.startsWith("CNHSBC")) {
            return result;
        }
        if (result.contains("-")) {
            String[] ss = result.split("-");

            while (ss[0].length() < 3) {
                ss[0] = "0" + ss[0];
            }
            if (ss[0].length() > 3) {
                ss[0] = ss[0].substring(0,3);
            }
            while (ss[1].length() < 6) {
                ss[1] = "0" + ss[1];
            }
            if (ss[1].length() > 6) {
                ss[1] = ss[1].substring(0,6);
            }
            result = ss[0] + ss[1];
        }
        result = "CNHSBC" + result;
        if (result.length() >15) {
            result = result.substring(0,15);
        }
        return result;
    }

    public static String formatJPY(String ccy, String src) {
        String result = src;
        if (ccy == null) {
            return src;
        }
        if (!ccy.equals("JPY") && !ccy.equals("KRW") ) {
            return result;
        }
        if (!src.contains(".")) {
            result = src + "00";
        } else {
            if (src.indexOf(".") == src.length()-1) {
                result = src.substring(0, src.indexOf(".")) + "00";
            } else if (src.indexOf(".") == src.length()-2) {
                result = src.substring(0, src.indexOf(".")) + src.substring(src.length()-1) + "0";
            } else if (src.indexOf(".") == src.length()-3) {
                result = src.substring(0, src.indexOf(".")) + src.substring(src.length()-2);
            }
        }
        return result;
    }

    public static String checkTyjdTenor(String sdays, Map<String, String> dict) {
        if (sdays.equals("")) {
            if (dict.containsKey("")) {
                return dict.get("");
            }
        }
        int days = Integer.parseInt(sdays);
        String result = "";
        for (String key : dict.keySet()) {
            if (key.contains("-")) {
                String[] ss = key.split("-");
                int s0 = Integer.parseInt(ss[0]);
                int s1 = Integer.parseInt(ss[1]);
                if (days >= s0 && days <= s1) {
                    return dict.get(key);
                }
            } else if (key.startsWith(">")) {
                int s0 = Integer.parseInt(key.substring(1));
                if (days >= s0) {
                    return dict.get(key);
                }
            } else {
                int s0 = Integer.parseInt(key);
                if (days == s0) {
                    return dict.get(key);
                }
            }
        }
        return result;
    }

    public static Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    public static String getMap(String type_no, String key) {
        String result = "";
        result = map.get(type_no).get(key);
        if (result == null || result.equals("null")) {
            result = "";
        }
        return result;
    }
}
