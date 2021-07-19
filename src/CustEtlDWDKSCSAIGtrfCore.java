package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_DWDKFK;
import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_DWDKJC;
import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_DWDKYE;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlDWDKSCSAIGtrfCore {

    private final EtlInsertService insertService;

    public CustEtlDWDKSCSAIGtrfCore(EtlInsertService insertService) {
        this.insertService = insertService;
    }

    private List<String> addFtyscsaiBase(String now, Map<String, Object> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(getString(src.get("C3BLRF")));
        result.add(getString(src.get("C3BLRF")));
        result.add("F082");
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));
        result.add(getString(src.get("C3RCDT")));
        result.add(getString(src.get("C3DUDT")));
        if (src.get("C3INVA").equals("0")) {
            result.add(now);
        } else {
            result.add("");
        }

        int days = differentDaysByMillisecond(src.get("C3DUDT"), src.get("C3ISDT"));
        result.add(checkTyjdTenor(String.valueOf(days), map.get("X0")));
        result.add("RF01");
        String c3inty = getString(src.get("C3INTY"));
        String c3cycd = getString(src.get("C3CYCD"));
        String c3cuno = getString(src.get("C3CUNO"));
        String effectiveDate = getMap("X34", c3cuno);
        days = differentDaysByMillisecond(src.get("C3RCDT"), effectiveDate);
        if (days > 0 && c3cycd.equals("CNY")) {
            c3inty = "LP1";
        }
        result.add(getMap("X33", c3inty));
        result.add("");
        result.add(getString(src.get("C3INMG")));
        result.add("");
        String purposeCode = getMap("X35", src.get("C3CUNO"));
        if (purposeCode != null && purposeCode.length() >= 3) {
            result.add(getMap("X36", purposeCode.substring(0, 3)));
        } else {
            //System.out.println("No PurposeCode:>>>" + src.get("C3BLRF"));
            result.add("");
        }
        result.add("01");
        result.add("100");
        return result;
    }

    private List<String> addFtyscsaiBalance(String now, Map<String, Object> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(getString(src.get("C3BLRF")));
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));
        result.add(getString(src.get("C3CYCD")));
        result.add(formatJPY(src.get("C3CYCD"),src.get("C3INVA")));
        return result;
    }

    private List<String> addFtyscsaiOccur(String now, Map<String, Object> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(getString(src.get("C3BLRF")));
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));


        result.add(getString(src.get("C3BLRF"))+getString(src.get("交易方向")));
        result.add(getString(src.get("C3RCDT")));
        result.add(getString(src.get("C3CYCD")));
        result.add(formatJPY(src.get("BBPRCY"),src.get("C3BLAM")));
        result.add("0");
        result.add(getString(src.get("C3INMG")));
        return result;
    }

    public void processFTYSCSAI(String now, List<Map<String, Object>> lstNow, List<Map<String, Object>> lstPrevious,
                                String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            String billref = getString(record.get("C3BLRF"));
            String c3isdt = getString(record.get("C3ISDT"));
            if (c3isdt == null || c3isdt.equals("0")) {
                continue;
            }
            boolean find = false;
            boolean matchOccur = false;
            boolean matchBase = false;
            String advos = getString(record.get("C3INVA"));
            if (advos != null && !advos.equals("0")) {
                matchBase = true;
            }
            for (Map<String, Object> orecord : lstPrevious) {
                String oc3isdt = getString(orecord.get("C3ISDT"));
                if (oc3isdt == null || oc3isdt.equals("0")) {
                    continue;
                }
                String obillref = getString(orecord.get("C3BLRF"));
                if (obillref.equals(billref)) {
                    find = true;
                    String badvos = getString(orecord.get("C3INVA"));
                    if (badvos == null || badvos.equals("0")) {
                        break;
                    }
                    if (new BigDecimal(advos).compareTo(new BigDecimal(badvos)) == 1) {
                        matchOccur = true;
                        matchBase = true;
                    }
                    break;
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
                base.add(addFtyscsaiBase(now, record));
                balance.add(addFtyscsaiBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addFtyscsaiOccur(now, record));
            }
        }
        for (Map<String, Object> record : lstPrevious) {
            String billref = getString(record.get("C3BLRF"));
            String c3isdt = getString(record.get("C3ISDT"));
            if (c3isdt == null || c3isdt.equals("0")) {
                continue;
            }
            boolean find = false;
            boolean matchBase = false;
            String advos = getString(record.get("C3INVA"));
            for (Map<String, Object> orecord : lstNow) {
                String oc3isdt = getString(orecord.get("C3ISDT"));
                String obillref = getString(orecord.get("C3BLRF"));
                if (obillref.equals(billref)) {
                    find = true;
                    String badvos = getString(orecord.get("C3INVA"));
                    if (badvos == null || badvos.equals("0")) {
                        matchBase = true;
                    }
                    break;
                }
            }
            if (!find) {
                matchBase = true;
            }
            if (matchBase) {
                record.put("交易方向", "0");
                base.add(addFtyscsaiBase(now, record));
                balance.add(addFtyscsaiBalance(now, record));
            }
        }
        insertService.insertData(SQL_DWDKFK, groupId, groupId, occur);
        insertService.insertData(SQL_DWDKYE, groupId, groupId, balance);
        insertService.insertData(SQL_DWDKJC, groupId, groupId, base);
    }
}
