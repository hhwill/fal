package com.gingkoo.imas.hsbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlUtils.*;
import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_TYJDFS;
import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_TYJDJC;
import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_TYJDYE;

@Component
public class CustEtlTYJDGtrfCore {

    private final EtlInsertService insertService;

    public CustEtlTYJDGtrfCore(EtlInsertService insertService) {
        this.insertService = insertService;
    }

    private List<String> addTyjdBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(getMap("X13", src.get("BAACSN")));
        result.add(src.get("BILLREF"));
        result.add("A01");
        result.add(src.get("BBDTAV"));
        result.add(src.get("BBDUDT"));
        //TODO 结清填当天
        result.add("");
        String days = src.get("BBUSAN");
        result.add(checkTyjdTenor(days, map.get("X14")));
        result.add("RF01");
        result.add(src.get("BBDRSP"));
        result.add(getMap("X12", src.get("BBDRTY")));
        result.add("");
        result.add("04");
        result.add("");
        return result;
    }

    private List<String> addTyjdBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addTyjdOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));

        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(now);
        result.add(src.get("BBPRCY"));
        result.add(src.get("BBDRSP"));
        result.add(src.get("BILLAMT"));
        result.add(src.get("交易方向"));
        return result;
    }

    public void processTYJD(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious,
                            String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("BILLREF");
            if (billref == null || !(billref.startsWith("SBN") || billref.startsWith("MIR")) ) {
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
                if (billref == null || !(billref.startsWith("SBN") || billref.startsWith("MIR")) ) {
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
                    if (!advos.equals(badvos)) {
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
                base.add(addTyjdBase(now, record));
                balance.add(addTyjdBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addTyjdOccur(now, record));
            }
        }
        insertService.insertData(SQL_TYJDFS, groupId, groupId, occur);
        insertService.insertData(SQL_TYJDYE, groupId, groupId, balance);
        insertService.insertData(SQL_TYJDJC, groupId, groupId, base);
    }

}
