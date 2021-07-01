package com.gingkoo.imas.hsbc.service;

import java.sql.*;
import java.util.*;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.gingkoo.root.facility.spring.tx.TransactionHelper;

@Component
public class CustLoadFileProcessService {

    private final JdbcTemplate jdbcTemplate;

    private CustEtlPJTXGtrfCore etlPJTXGtrfCore;
    private CustEtlTYJDGtrfCore etlTYJDGtrfCore;
    private CustEtlDWDKGtrfCore etlDWDKGtrfCore;
    private CustEtlDWDKSCSAIGtrfCore etlDWDKSCSAIGtrfCore;
    private CustEtlGRKHXX etlGRKHXX;
    private CustEtlWCAS etlWCAS;

    public CustLoadFileProcessService(CustEtlPJTXGtrfCore etlPJTXGtrfCore, CustEtlTYJDGtrfCore etlTYJDGtrfCore,
                                      CustEtlDWDKGtrfCore etlDWDKGtrfCore,
                                      CustEtlDWDKSCSAIGtrfCore etlDWDKSCSAIGtrfCore,
                                      CustEtlGRKHXX etlGRKHXX,CustEtlWCAS etlWCAS,
                                      TransactionHelper transactionTemplate,
                                      DataSource dataSource) {
        this.etlPJTXGtrfCore = etlPJTXGtrfCore;
        this.etlTYJDGtrfCore = etlTYJDGtrfCore;
        this.etlDWDKGtrfCore = etlDWDKGtrfCore;
        this.etlDWDKSCSAIGtrfCore = etlDWDKSCSAIGtrfCore;
        this.etlGRKHXX = etlGRKHXX;
        this.etlWCAS = etlWCAS;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public List<Map<String, String>> handle(List<Map<String,Object>> records) throws SQLException {
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();

        for (Map<String, Object> record : records) {
            Map<String, String> newrecord = new HashMap<String, String>();
            for (String key : record.keySet()) {
                Object o = record.get(key);
                if (o == null) {
                    newrecord.put(key, "");
                } else {
                    newrecord.put(key, o.toString());
                }
            }
            result.add(newrecord);
        }
        return result;
    }

    private void initMap() throws Exception {
        if (EtlUtils.map.size() == 0) {
            String sql = "select * from map_info";
            List<Map<String, String>> lst = handle(jdbcTemplate.queryForList(sql));
            for (Map<String, String> record : lst) {
                Map<String, String> srecord = new HashMap<String, String>();
                if (EtlUtils.map.containsKey(record.get("TYPE_NO"))) {
                    srecord = EtlUtils.map.get(record.get("TYPE_NO"));
                }
                srecord.put(record.get("SRC"), record.get("DEST"));
                EtlUtils.map.put(record.get("TYPE_NO"), srecord);
            }
        }
    }

    public boolean process(String type, String now, String group_id) throws Exception {
        initMap();
        String sql = "select * from " + type + " where data_date = '" + now + "' and group_id = '" + group_id + "'";
        List<Map<String, String>> lst = handle(jdbcTemplate.queryForList(sql));
        sql = "select max(data_date) from " + type + " where data_date < '" + now + "'";
        String previous = jdbcTemplate.queryForObject(sql, String.class);
        sql = "select * from " + type + " where data_date = '" + previous + "' and group_id = '" + group_id + "'";
        List<Map<String, String>> lst1 = new ArrayList<Map<String, String>>();
        if (!type.contains("GRKHXX")) {
            lst1 = handle(jdbcTemplate.queryForList(sql));
        }
        if (type.equals("ODS_GTRF_PJTX")) {
            etlPJTXGtrfCore.processPJTX(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GTRF_TYJD")) {
            etlTYJDGtrfCore.processTYJD(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GTRF_FTYDWDK")) {
            etlDWDKGtrfCore.processFTYDWDK(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GTRF_FTYSCSAI")) {
            etlDWDKSCSAIGtrfCore.processFTYSCSAI(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GRKHXX_BOSC")) {
            etlGRKHXX.processGRKHXXBASE(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GRHXXX")) {
            etlGRKHXX.processGRKHXX(now, lst, lst1, group_id);
        }else if (type.equals("GTRF_CORPCUSLVL")) {
            etlWCAS.processWCAS_DGHKXX(now, lst, lst1, group_id);
        } else if (type.equals("GTRF_CORPDDAC")) {
            etlWCAS.processCORPDDAC(now, lst, lst1, group_id);
        } else if (type.equals("GTRF_CORPTDAC3")) {
            etlWCAS.processCORPTDAC3(now, lst, lst1, group_id);
        }
        return true;
    }
}
