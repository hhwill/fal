package com.gingkoo.imas.hsbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_GRKHXX;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlGRKHXX {

    private final EtlInsertService insertService;

    public CustEtlGRKHXX(EtlInsertService insertService) {
        this.insertService = insertService;
    }


    private List<String> addGRKHXXBASE(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatKHH(src.get("客户号")+"-"+src.get("客户号1")));
        String nbjgh = formatNBJGH(src.get("内部机构号"));
        result.add(formatNBJGH(src.get("内部机构号")));
        String mode = "";
        String id = src.get("常住地行政区划代码");
        if (id != null && id.length() == 18) {
            mode = id.substring(0,6);
        } else {
            //所属内部机构的地区代码
            mode = getMap("XDQDM", nbjgh);
        }
        result.add(mode);
        String sxed = src.get("授信额度");
        if (sxed == null || sxed.trim().equals("")) {
            sxed = "0";
        }
        result.add(sxed);
        String yyed = src.get("已用额度");
        if (yyed == null || yyed.trim().equals("")) {
            yyed = "0";
        }
        result.add(yyed);
        result.add(src.get("客户细类"));
        result.add(src.get("农户标志"));
        return result;
    }

    private List<String> addGRKHXX(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatKHH(src.get("客户号")));
        result.add(formatNBJGH(src.get("内部机构号")));

        String mode = "";
        String id = src.get("");
        if (id != null && id.length() == 18) {
            mode = id.substring(0,6);
        } else {
            //所属内部机构的地区代码
            mode = getMap("XDQDM", formatKHH(src.get("客户号")));
        }
        result.add(mode);

        String sxed = src.get("授信额度");
        if (sxed == null || sxed.trim().equals("")) {
            sxed = "0";
        }
        result.add(sxed);
        String yyed = src.get("已用额度");
        if (yyed == null || yyed.trim().equals("")) {
            yyed = "0";
        }
        result.add(yyed);
        result.add(src.get("客户细类"));
        result.add(src.get("农户标志"));
        return result;
    }

    public void processGRKHXXBASE(String now, List<Map<String, String>> lstNow,
                                  List<Map<String, String>> lstPrevious, String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            base.add(addGRKHXXBASE(now, record));
        }
        insertService.insertData(SQL_GRKHXX, groupId, groupId, base);
    }

    public void processGRKHXX(String now, List<Map<String, String>> lstNow,
                              List<Map<String, String>> lstPrevious, String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            base.add(addGRKHXX(now, record));
        }
        insertService.insertData(SQL_GRKHXX, groupId, groupId, base);
    }
}
