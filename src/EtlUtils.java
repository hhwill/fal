package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

import org.springframework.security.access.method.P;

public class EtlUtils {

    public static int differentDaysByMillisecond(Object date1, Object date2)
    {
        try {
            Date ddate1 = new SimpleDateFormat("yyyyMMdd").parse(date1.toString());
            Date ddate2 = new SimpleDateFormat("yyyyMMdd").parse(date2.toString());
            int days = (int) ((ddate2.getTime() - ddate1.getTime()) / (1000 * 3600 * 24));
            return Math.abs(days);
        } catch (Exception ex) {
            return 0;
        }
    }

    public static String formatCKZHBH(Object s1, Object s2, Object s3) {
        String ss1 = "";
        String ss2 = "";
        String ss3 = "";
        if (s1 != null) {
            ss1 = s1.toString();
        }
        if (s2 != null) {
            ss2 = s2.toString();
        }
        if (s3 != null) {
            ss3 = s3.toString();
        }
        String[] ss = new String[3];
        ss[0] = ss1;
        ss[1] = ss2;
        ss[2] = ss3;
        String result = ss1;
        if (ss1.contains("-") && (ss1.indexOf("-") != ss1.lastIndexOf("-"))) {
            ss = ss1.split("\\-");
        }
        while (ss[0].length() < 3) {
            ss[0] = "0" + ss[0];
        }
        while (ss[1].length() < 6) {
            ss[1] = "0" + ss[1];
        }
        while (ss[2].length() < 3) {
            ss[2] = "0" + ss[2];
        }
        result = ss[0] + ss[1] + ss[2];
        if (!result.startsWith("CNHSBC")) {
            result = "CNHSBC"+result;
        }
        return result;
    }

    public static String formatNBJGH(Object src) {
        String ssrc = "";
        if (src != null) {
            ssrc = src.toString();
        }
        String result = ssrc;
        while (result.length() < 3) {
            result = "0" + result;
        }
        if (result.length() == 3) {
            result = "CNHSBC"+ result;
        }
        String dest = getMap("NBJGH", result);
        if (!dest.equals("")) {
            result = dest;
        }
        return result;
    }

    public static String formatKHH(Object src) {
        String result = "";
        if (src != null) {
            result = src.toString();
        }
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

    public static String formatJPY(Object ccy, Object s) {
        String src = "";
        if (s != null) {
            src = s.toString();
        }
        String sccy = "";
        if (ccy != null) {
            sccy = ccy.toString();
        }
        String result = src;
        if (ccy == null) {
            return src;
        }
        if (!sccy.equals("JPY") && !sccy.equals("KRW") ) {
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

    public static String checkWcasTendor(String sdays, Map<String, String> dict) {
        String result = "";
        int days = 0;
        try {
            days = Integer.parseInt(sdays);
        } catch (Exception ex) {

        }
        for (String key : dict.keySet()) {
            if (key.contains("<") && (key.indexOf("<") == key.lastIndexOf("<"))) {
                String[] ss = key.split("\\<");
                if (days < Integer.parseInt(ss[1])) {
                    return dict.get(key);
                }
            } else if (key.contains("<")) {
                String[] ss = key.split("\\<");
                if (days > Integer.parseInt(ss[0]) && days < Integer.parseInt(ss[2])) {
                    return dict.get(key);
                }
            } else if (key.contains("=")) {
                String[] ss = key.split("\\=");
                if (days == Integer.parseInt(ss[1])) {
                    return dict.get(key);
                }
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

    //交易流水 交易日期 发生金额 交易方向 的列表
    public static List<List<String>> getWCASJYLS(String now, Map<String, Object> src) {
        List<List<String>> result = new ArrayList<List<String>>();
        String THCPDT = getString(src.get("THCPDT"));
        String THCPWS = getString(src.get("THCPWS"));
        String THDLNO = getString(src.get("THDLNO"));
        while (THDLNO.length() < 5) {
            THDLNO = "0" + THDLNO;
        }
        List<String> record = new ArrayList<String>();
        if (THCPDT.equals(now) && src.get("THDLCD").equals("NEW")) {
            record.add(THCPDT+THCPWS+THDLNO);
            record.add(getString(src.get("THCPDT")));
            record.add(getString(src.get("TRANAMT")));
            record.add("1");
            result.add(record);
        } else if (THCPDT.equals(now) && src.get("THDLCD").equals("WTH")) {
            record.add(THCPDT+THCPWS+THDLNO);
            record.add(getString(src.get("THCPDT")));
            String TRANAMT = getString(src.get("TRANAMT"));
            if (TRANAMT.startsWith("-")) {
                TRANAMT = TRANAMT.substring(1);
            }
            record.add(new BigDecimal(TRANAMT).add(new BigDecimal(getString(src.get("INTACCRUED")))).toString());
            record.add("0");
            result.add(record);
        } else if (THCPDT.equals(now) && src.get("THDLCD").equals("RNW")) {
            record.add(THCPDT+THCPWS+THDLNO);
            record.add(getString(src.get("THCPDT")));
            record.add(getString(src.get("LEDGER")));
            record.add("0");
            result.add(record);
            List<String> record1 = new ArrayList<String>();
            record1.add(THCPDT+THCPWS+THDLNO+"N");
            record1.add(getString(src.get("THCPDT")));
            record1.add(getString(src.get("LEDGER")));
            record1.add("1");
            result.add(record1);
        } else if (src.get("TDSTDT").equals(now) && src.get("THCPDT").equals("0") && src.get("TDMTIN").equals("2") && src.get("TDSTUS").equals("1")) {
            record.add(getString(src.get("TDSTDT"))+formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
            record.add(getString(src.get("TDSTDT")));
            record.add(getString(src.get("LEDGER")));
            record.add("0");
            result.add(record);
            List<String> record1 = new ArrayList<String>();
            record1.add(getString(src.get("TDSTDT"))+formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
            record1.add(getString(src.get("TDSTDT")));
            record1.add(getString(src.get("LEDGER")));
            record1.add("1");
            result.add(record1);
        }
        return result;
    }

    //实际利率 账户余额 存款序号 存款产品类别  的列表
    public static List<List<String>> getCKXH(Map<String, Object> src) {
        List<List<String>> result = new ArrayList<List<String>>();
        //默认不分层的返回
        List<String> record = new ArrayList<String>();
        String LEDGER = getString(src.get("LEDGER"));
        String DGCIBL = getString(src.get("DGCIBL"));

        String defCKCPLB = getMap("WCAS_ProductType", src.get("DFAPTY"));
        String DGCIRS = getString(src.get("DGCIRS"));
        if (DGCIRS == null || DGCIRS.trim().equals("")) {
            DGCIRS = "0";
        }

        String lastCKCPLB = defCKCPLB;
        String lastCKXH = "01";

        String INR1 = "X5INR1";
        String INR2 = "X5INR2";
        String INR3 = "X5INR3";
        String INR4 = "X5INR4";
        String BAL1 = "CRBAL1";
        String BAL2 = "CRBAL2";
        String BAL3 = "CRBAL3";
        String BAL4 = "CRBAL4";
        if (DGCIBL == null) {
            DGCIBL = "X";
        }
        if (DGCIBL.equals("Y")) {
            INR1 = "DRINR1";
            INR2 = "DRINR2";
            INR3 = "DRINR3";
            INR4 = "DRINR4";
            BAL1 = "CR_BAL1";
            BAL2 = "CR_BAL2";
            BAL3 = "CR_BAL3";
            BAL4 = "CR_BAL4";
        }
        String lastBAL = "0";
        if (DGCIBL.equals("N") || DGCIBL.equals("Y")) {
            String X5INR1 = getString(src.get(INR1));
            if (X5INR1.trim().equals("")) {
                X5INR1 = "0";
            }
            String X5INR2 = getString(src.get(INR2));
            if (X5INR2.trim().equals("")) {
                X5INR2 = "0";
            }
            String X5INR3 = getString(src.get(INR3));
            if (X5INR3.trim().equals("")) {
                X5INR3 = "0";
            }
            String X5INR4 = getString(src.get(INR4));
            if (X5INR4.trim().equals("")) {
                X5INR4 = "0";
            }
            if (X5INR2.equals("0") && X5INR3.equals("0") && X5INR4.equals("0")) {
                if (DGCIBL.equals("N")) {
                    record.add(new BigDecimal(DGCIRS).add(new BigDecimal(X5INR1)).toString());
                } else {
                    record.add(DGCIRS);
                }
                record.add(LEDGER);
                record.add(lastCKXH);
                record.add(defCKCPLB);
                result.add(record);
            } else {
                String X5BAL1 = getString(src.get(BAL1));
                String X5BAL2 = getString(src.get(BAL2));
                String X5BAL3 = getString(src.get(BAL3));
                String X5BAL4 = getString(src.get(BAL4));
                boolean hasBAL1 = false;
                if (!X5INR1.equals("0") ) {
                    hasBAL1 = true;
                    lastCKCPLB = "D051";
                }
                if (hasBAL1) {
                    if (DGCIBL.equals("N")) {
                        record.add(new BigDecimal(DGCIRS).add(new BigDecimal(X5INR1)).toString());
                    } else {
                        record.add(DGCIRS);
                    }
                    if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL1)) > 0) {
                        record.add(new BigDecimal(X5BAL1).toString());
                        record.add(lastCKXH);
                        lastCKXH = "02";
                        record.add(lastCKCPLB);
                        if (lastCKCPLB.equals("D0501")) {
                            lastCKCPLB = "D0502";
                        }
                        result.add(record);
                    } else {
                        record.add(LEDGER);
                        record.add(lastCKXH);
                        lastCKXH = "02";
                        record.add(defCKCPLB);
                        if (lastCKCPLB.equals("D0501")) {
                            lastCKCPLB = "D0502";
                        }
                        result.add(record);
                    }
                    lastBAL = X5BAL1;
                }
                if (!X5INR2.equals("0") && new BigDecimal(LEDGER).compareTo(new BigDecimal(lastBAL)) > 0) {
                    List<String> record2 = new ArrayList<String>();
                    if (DGCIBL.equals("N")) {
                        record2.add(new BigDecimal(DGCIRS).add(new BigDecimal(X5INR2)).toString());
                    } else {
                        record2.add(DGCIRS);
                    }
                    if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL2)) > 0) {
                        record2.add(X5BAL2);
                        record2.add(lastCKXH);
                        if (lastCKXH.equals("02")) {
                            lastCKXH = "03";
                        } else if (lastCKXH.equals("01")) {
                            lastCKXH = "02";
                        }
                        record2.add(lastCKCPLB);
                        result.add(record2);
                    } else {
                        if (new BigDecimal(LEDGER).compareTo(new BigDecimal(lastBAL)) > 0) {
                            record2.add(new BigDecimal(LEDGER).subtract(new BigDecimal(lastBAL)).toString());
                            record2.add(lastCKXH);
                            if (lastCKXH.equals("02")) {
                                lastCKXH = "03";
                            } else if (lastCKXH.equals("01")) {
                                lastCKXH = "02";
                            }
                            record2.add(lastCKCPLB);
                            if (lastCKCPLB.equals("D0501")) {
                                lastCKCPLB = "D0502";
                            }
                            result.add(record2);
                        }
                    }
                    lastBAL = X5BAL2;
                }
                if (!X5INR3.equals("0") && new BigDecimal(LEDGER).compareTo(new BigDecimal(lastBAL)) > 0) {
                    List<String> record3 = new ArrayList<String>();
                    if (DGCIBL.equals("N")) {
                        record3.add(new BigDecimal(DGCIRS).add(new BigDecimal(X5INR3)).toString());
                    } else {
                        record3.add(DGCIRS);
                    }
                    if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL3)) > 0) {
                        record3.add(X5BAL3);
                        record3.add(lastCKXH);
                        if (lastCKXH.equals("03")) {
                            lastCKXH = "04";
                        } else if (lastCKXH.equals("02")) {
                            lastCKXH = "03";
                        } else if (lastCKXH.equals("01")) {
                            lastCKXH = "02";
                        }
                        record3.add(lastCKCPLB);
                        result.add(record3);
                    } else {
                        if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL2)) > 0) {
                            record3.add(new BigDecimal(LEDGER).subtract(new BigDecimal(lastBAL)).toString());
                            record3.add(lastCKXH);
                            if (lastCKXH.equals("03")) {
                                lastCKXH = "04";
                            } else if (lastCKXH.equals("02")) {
                                lastCKXH = "03";
                            } else if (lastCKXH.equals("01")) {
                                lastCKXH = "02";
                            }
                            record3.add(lastCKCPLB);
                            if (lastCKCPLB.equals("D0501")) {
                                lastCKCPLB = "D0502";
                            }
                            result.add(record3);
                        }
                    }
                    lastBAL = X5BAL3;
                }
                if (!X5INR4.equals("0") && new BigDecimal(LEDGER).compareTo(new BigDecimal(lastBAL)) > 0) {
                    List<String> record4 = new ArrayList<String>();
                    if (DGCIBL.equals("N")) {
                        record4.add(new BigDecimal(DGCIRS).add(new BigDecimal(X5INR4)).toString());
                    } else {
                        record4.add(DGCIRS);
                    }
                    if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL4)) > 0) {
                        record4.add(X5BAL4);
                        record4.add(lastCKXH);
                        record4.add(lastCKCPLB);
                        result.add(record4);
                    } else {
                        if (new BigDecimal(LEDGER).compareTo(new BigDecimal(X5BAL3)) > 0) {
                            record4.add(new BigDecimal(LEDGER).subtract(new BigDecimal(lastBAL)).toString());
                            record4.add(lastCKXH);
                            record4.add(lastCKCPLB);
                            result.add(record4);
                        }
                    }
                }
            }
        } else {
            record.add(DGCIRS);
            record.add(LEDGER);
            record.add("01");
            record.add(defCKCPLB);
            result.add(record);
        }
        return result;
    }

    public static Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    public static String getMap(String type_no, Object key) {
        String skey = "";
        if (key != null) {
            skey = key.toString();
        }
        String result = "";
        result = map.get(type_no).get(skey);
        if (result == null || result.equals("null")) {
            result = "";
        }
        return result;
    }

    public static String getString(Object s) {
        if (s == null) {
            return "";
        }
        return s.toString();
    }
}
