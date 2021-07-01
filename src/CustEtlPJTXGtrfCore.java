package com.gingkoo.imas.hsbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlUtils.*;
import static com.gingkoo.imas.hsbc.service.EtlConst.*;

@Component
public class CustEtlPJTXGtrfCore {

    private final EtlInsertService insertService;

    public CustEtlPJTXGtrfCore(EtlInsertService insertService) {
        this.insertService = insertService;
    }

    private List<String> addBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));


        result.add(getMap("X2", src.get("BILLREF").substring(0,3)));
        result.add(src.get("BILLREF"));
        result.add(getMap("X1", src.get("BILLREF").substring(0,3)));
        result.add(src.get("BBDTAV"));
        result.add(src.get("BBDUDT"));

        int daydiff = differentDaysByMillisecond(src.get("BBDUDT"),src.get("BBINSD"));
        if (daydiff <= 90) {
            result.add("01");
        } else if (daydiff <= 180) {
            result.add("02");
        } else if (daydiff <= 365){
            result.add("03");
        } else {
            result.add("");
        }
        result.add(src.get("BBDRSP"));
        return result;
    }

    private List<String> addBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(src.get("BILLREF"));
        result.add(src.get("BBPRCY"));
        result.add(src.get("ADVOS"));
        return result;
    }

    private List<String> addOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(src.get("BILLREF"));
        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(src.get("BBPRCY"));
        result.add(now);
        result.add(src.get("BILLAMT"));
        result.add(src.get("BBDRSP"));
        result.add(src.get("交易方向"));
        return result;
    }

    public void processPJTX(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious,
                            String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("BILLREF");
            if (billref == null || !(billref.startsWith("BBE") || billref.startsWith("XBG") || billref.startsWith("FAO") || billref.startsWith("FAW") || billref.startsWith("FAT") || billref.startsWith("BAT") || billref.startsWith("BDP") || billref.startsWith("DPF") || billref.startsWith("BBS"))) {
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
                if (obillref == null || !(obillref.startsWith("BBE") || obillref.startsWith("XBG") || obillref.startsWith("FAO") || obillref.startsWith("FAW") || obillref.startsWith("FAT") || obillref.startsWith("BAT") || obillref.startsWith("BDP") || obillref.startsWith("DPF") || obillref.startsWith("BBS"))) {
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
                base.add(addBase(now, record));
                balance.add(addBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addOccur(now, record));
            }
        }
        insertService.insertData(SQL_PJTXFS, groupId, groupId, occur);
        insertService.insertData(SQL_PJTXYE, groupId, groupId, balance);
        insertService.insertData(SQL_PJTXJC, groupId, groupId, base);
    }
}
