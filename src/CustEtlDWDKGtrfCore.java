package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlUtils.*;
import static com.gingkoo.imas.hsbc.service.EtlConst.*;


@Component
public class CustEtlDWDKGtrfCore {

    private final EtlInsertService insertService;

    public CustEtlDWDKGtrfCore(EtlInsertService insertService) {
        this.insertService = insertService;
    }

    private List<String> addFtydwdkBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(src.get("BILLREF"));
        String zzlf04 = src.get("ZZLF04");
        if (zzlf04 != null) {
            if (zzlf04.equals("1")) {
                result.add("F082");
            } else if (zzlf04.equals("0")) {
                result.add("F081");
            } else {
                result.add("");
            }
        } else {
            result.add("");
        }
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(src.get("BBDTAV"));
        result.add(src.get("BBDUDT"));
        //TODO 结清填当天
        if (src.get("ADVOS").equals("0"))
            result.add(now);
        else
            result.add("");
        String days = src.get("BBUSAN");
        result.add(checkTyjdTenor(days, map.get("X0")));
        result.add("RF01");
        result.add(getMap("X22", src.get("BBDRTY")));
        result.add("");
        result.add(src.get("BBDRSP"));
        result.add("");
        String zzlf05 = src.get("ZZLF05");
        if (zzlf05 != null && zzlf05.length() >= 3) {
            result.add(getMap("X24",  zzlf05.substring(0,3)));
        } else{
            result.add("");
        }
        result.add("01");
        result.add("");
        return result;
    }

    private List<String> addFtydwdkBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addFtydwdkOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));


        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(now);
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("BILLAMT")));
        result.add("");
        String bbdrsp = src.get("BBDRSP");
        if (src.get("BBDRTY").equals("LP1")) {
            bbdrsp =  new BigDecimal(bbdrsp).add(new BigDecimal("3.85")).toString();
        }
        result.add(bbdrsp);
        return result;
    }

    public void processFTYDWDK(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious, String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("BILLREF");
            //保证金
            if (billref == null || billref.startsWith("M") || billref.startsWith("YM") ) {
                continue;
            }
            String productType = getMap("X21", billref.substring(0,3));
            if (productType.equals("同业")||productType.equals("票据贴现及转贴现")||productType.equals("GTE")||productType.equals(
                    "Standby DC")||productType.equals("DC")||productType.equals("BADI")) {
                continue;
            }
            String bbprcy = record.get("BBPRCY");
            if (bbprcy == null || !(bbprcy.startsWith("JPY") || bbprcy.startsWith("USD") || bbprcy.startsWith("EUR") || bbprcy.startsWith("HKD")) ) {
                continue;
            }
            boolean find = false;
            boolean matchOccur = false;
            boolean matchBase = false;
            String advos = record.get("ADVOS");
            if (advos != null && !advos.equals("0")) {
                matchBase = true;
            }
            for (Map<String, String> orecord : lstPrevious) {
                String obillref = orecord.get("BILLREF");
                if (obillref == null || obillref.startsWith("M") || billref.startsWith("YM") ) {
                    continue;
                }
                String oproductType = getMap("X21", obillref.substring(0,3));
                if (oproductType.equals("同业")||oproductType.equals("票据贴现及转贴现")||oproductType.equals("GTE")||oproductType.equals(
                        "Standby DC")||oproductType.equals("DC")||oproductType.equals("BADI")) {
                    continue;
                }
                String obbprcy = orecord.get("BBPRCY");
                if (obbprcy == null || !(obbprcy.startsWith("JPY") || obbprcy.startsWith("USD") || obbprcy.startsWith("EUR") || obbprcy.startsWith("HKD")) ) {
                    continue;
                }
                if (obillref.equals(billref)) {
                    find = true;
                    String badvos = orecord.get("ADVOS");
                    if (badvos == null || badvos.equals("0")) {
                        break;
                    }
                    if (new BigDecimal(advos).compareTo(new BigDecimal(badvos)) == 1) {
                        matchOccur = true;
                        matchBase = true;
                        break;
                    }
                }
            }
            if (!find) {
                matchOccur = true;
            }
            if (matchBase) {
                if (advos.equals("0")) {
                    record.put("交易方向", "0");
                } else {
                    record.put("交易方向", "1");
                }
                base.add(addFtydwdkBase(now, record));
                balance.add(addFtydwdkBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addFtydwdkOccur(now, record));
            }
        }
        insertService.insertData(SQL_DWDKFK, groupId, groupId, occur);
        insertService.insertData(SQL_DWDKYE, groupId, groupId, balance);
        insertService.insertData(SQL_DWDKJC, groupId, groupId, base);
    }
}
