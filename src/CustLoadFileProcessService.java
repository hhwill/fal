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

    private void initMap(String now) throws Exception {
        EtlUtils.map.clear();
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
            sql = "select NBJGH, DQDM from imas_pm_jgfzxx where sjrq = '"+now+"'";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> dqdm = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                dqdm.put(record.get("NBJGH"), record.get("DQDM"));
            }
            EtlUtils.map.put("XDQDM", dqdm);
            sql = "select DATA_NO from gp_bm_data_dic where data_type_no = 'C_REGION_CODE'";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> dqqhdm = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                dqqhdm.put(record.get("DATA_NO"), record.get("DATA_NO"));
            }
            EtlUtils.map.put("DQQHDM", dqqhdm);
            sql = "select src,dest from map_nbjgh where data_date = '"+now+"'";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> nbjgh = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                nbjgh.put(record.get("SRC"), record.get("DEST"));
            }
            EtlUtils.map.put("NBJGH", dqqhdm);
            sql = "select * from MAP_WCAS_RATE_TYPE";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> rateType = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                rateType.put(record.get("ID1")+"_"+record.get("ID2")+"_"+record.get("ID3"),
                        record.get("DJJZLX")+"|"+record.get("LVLX")+"|"+record.get("JZLV")+"|"+record.get("LVFDPL"));
            }
            EtlUtils.map.put("RATETYPE", rateType);
            sql = "select * from MAP_WCAS_RATE where data_date ='"+now+"'";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> rate = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                rate.put(record.get("PAIR"), record.get("XESTOR"));
            }
            EtlUtils.map.put("RATE", rate);
            sql = "select ZIACB,ZIACS,ZIACX, ZIDTAS from ODS_WCAS_CLOSEDAC";
            lst = handle(jdbcTemplate.queryForList(sql));
            Map<String, String> closedac = new HashMap<String, String>();
            for (Map<String, String> record : lst) {
                closedac.put(record.get("ZIACB")+record.get("ZIACS")+record.get("ZIACX"), record.get("ZIDTAS"));
            }
            EtlUtils.map.put("CLOSEDAC", closedac);
        }
    }

    public boolean process(String type, String now, String group_id) throws Exception {
        if(type.equals("TEST")) {
            etlWCAS.test();
            return true;
        }
        initMap(now);
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
        } else if (type.equals("ODS_BOSC_GRKHXX")) {
            etlGRKHXX.processGRKHXXBASE(now, lst, lst1, group_id);
        } else if (type.equals("ODS_GRHXXX")) {
            etlGRKHXX.processGRKHXX(now, lst, lst1, group_id);
        }else if (type.equals("ODS_WCAS_CORPCUSLVL")) {
            etlWCAS.processWCAS_DGHKXX(now, lst, lst1, group_id);
        } else if (type.equals("ODS_WCAS_CORPDDAC")) {
            etlWCAS.processCORPDDAC(now, lst, lst1, group_id);
        } else if (type.equals("ODS_WCAS_CORPTDAC3")) {
            etlWCAS.processCORPTDAC3(now, lst, lst1, group_id);
        }
        return true;
    }
}
