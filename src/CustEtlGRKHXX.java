package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import com.gingkoo.root.facility.spring.tx.TransactionHelper;

import static com.gingkoo.imas.hsbc.service.EtlConst.SQL_GRKHXX;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlGRKHXX {

    private final Logger logger = LoggerFactory.getLogger(CustEtlGRKHXX.class);

    private final EtlInsertService insertService;

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    public CustEtlGRKHXX(EtlInsertService insertService, TransactionHelper transactionTemplate, DataSource dataSource) {
        this.insertService = insertService;
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    private List<String> addGRKHXXBASE(String now, Map<String, Object> src, String group_id) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        String khh = formatKHH(src.get("客户号")+"-"+src.get("客户号1"));
        result.add(khh);
        String nbjgh = getString(src.get("内部机构号"));
        if (nbjgh.equals("")) {
            nbjgh = khh.substring(0,9);
        } else {
            nbjgh = formatNBJGH(nbjgh);
        }
        result.add(nbjgh);
        String mode = "";
        String id = getString(src.get("常住地行政区划代码"));
        if (id != null && id.length() == 18) {
            mode = id.substring(0,6);
        } else {
            //所属内部机构的地区代码
            mode = getMap("XDQDM", nbjgh);
        }
        result.add(mode);
        String sxed = getString(src.get("授信额度"));
        if (sxed == null || sxed.trim().equals("")) {
            sxed = "";
        }
        result.add(sxed);
        String yyed = getString(src.get("已用额度"));
        if (yyed == null || yyed.trim().equals("")) {
            yyed = "";
        }
        result.add(yyed);
        result.add(getString(src.get("客户细类")));
        result.add(getString(src.get("农户标志")));
        result.add(group_id);
        return result;
    }

    private List<String> addGRKHXX(String now, Map<String, Object> src, String group_id) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        String khh = formatKHH(src.get("客户号"));
        result.add(khh);
        String nbjgh = getString(src.get("内部机构号"));
        if (nbjgh.equals("")) {
            nbjgh = khh.substring(0,9);
        } else {
            nbjgh = formatNBJGH(nbjgh);
        }
        result.add(nbjgh);

        String mode = "";
        String id = getString(src.get("常住地行政区划代码"));
        if (id != null && id.trim().length() == 18) {
            mode = id.substring(0,6);
        } else {
            //所属内部机构的地区代码
            mode = getMap("XDQDM", formatKHH(src.get("客户号")));
        }
        result.add(mode);

        String sxed = getString(src.get("授信额度"));
        if (sxed == null || sxed.trim().equals("")) {
            sxed = "0";
        }
        result.add(sxed);
        String yyed = getString(src.get("已用额度"));
        if (yyed == null || yyed.trim().equals("")) {
            yyed = "0";
        }
        result.add(yyed);
        result.add(getString(src.get("客户细类")));
        result.add(getString(src.get("农户标志")));
        result.add(group_id);
        return result;
    }

    public void processGRKHXXBASE(String now, List<Map<String, Object>> lstNow, String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            base.add(addGRKHXXBASE(now, record, groupId));
        }
        insertService.insertData(SQL_GRKHXX, groupId, groupId, base);
    }

    public void processGRKHXX(String now, List<Map<String, Object>> lstNow, String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            base.add(addGRKHXX(now, record, groupId));
        }
        insertService.insertData(SQL_GRKHXX, groupId, groupId, base);
        /*
                    "`SJRQ`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`CZDHZQHDM`,\n" +
            "`SXED`,\n" +
            "`YYED`,\n" +
            "`KHXL`,\n" +
            "`NHBZ`,\n" +
         */

    }

    public void processOtherInfo(String now, List<Map<String, Object>> lstNow, String groupId) throws Exception {
        for (int i = 0; i < lstNow.size(); i++) {
            Map<String, Object> rnow = lstNow.get(i);
            boolean find = false;
            String khh = getString(rnow.get("客户号"));
            if (i > 0) {
                for (int j = 0; j < i; j++) {
                    if (khh.equals(lstNow.get(j).get("客户号"))) {
                        find = true;
                    }
                }
            }
            String sqlPrefix = "update imas_pm_GRKHXX set ";
            String sqlMiddle = "";
            String nbjgh = getString(rnow.get("内部机构号"));
            if (!nbjgh.equals("")) {
                nbjgh = formatNBJGH(nbjgh);
                sqlMiddle += " nbjgh = '" + nbjgh + "',";
            }
            String CZDHZQHDM = getString(rnow.get("常住地行政区划代码"));
            if (!CZDHZQHDM.equals("")) {
                sqlMiddle += " CZDHZQHDM = '" + CZDHZQHDM + "',";
            }
            String SXED = getString(rnow.get("授信额度"));
            if (!SXED.equals("")) {
                if (find) {
                    sqlMiddle += " SXED = SXED+" + SXED + ",";
                } else {
                    sqlMiddle += " SXED = " + SXED + ",";
                }
            }
            String YYED = getString(rnow.get("已用额度"));
            if (!YYED.equals("")) {
                if (find) {
                    sqlMiddle += " YYED = YYED+" + YYED + ",";
                } else {
                    sqlMiddle += " YYED = " + YYED + ",";
                }
            }
            String KHXL = getString(rnow.get("客户细类"));
            if (!KHXL.equals("")) {
                sqlMiddle += " KHXL = '" + KHXL + "',";
            }
            String NHBZ = getString(rnow.get("农户标志"));
            if (!NHBZ.equals("")) {
                sqlMiddle += " NHBZ = '" + NHBZ + "',";
            }
            sqlMiddle += "group_id = '" + getString(rnow.get("GROUP_ID")) + "' ";
            if (!sqlMiddle.equals("")) {
                String sql = sqlPrefix + sqlMiddle + " where sjrq = '"+now + "' " +
                        "and khh = '" + khh + "'";
                try {
                    jdbcTemplate.execute(sql);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public boolean process(String now, String group_id) throws Exception {
        logger.info(">>>Start GRKHXX " + now + " " + group_id);
        String sql = String.format("delete from imas_pm_grkhxx where sjrq = '%s'", now);
        execUpdSqlCommit(sql);
        sql = "select * from ODS_BOSC_GRKHXX where data_date = '" + now + "'";
        List<Map<String, Object>> lstNow = jdbcTemplate.queryForList(sql);
        processGRKHXXBASE(now, lstNow, group_id);
        sql =
                "select distinct `客户号`,`数据日期`,`内部机构号`,`常住地行政区划代码`,`授信额度`,`已用额度`,`客户细类`,`农户标志`,DATA_DATE,GROUP_ID from ODS_GRKHXX a " +
                        "where data_date = '" + now + "' and not exists (select * from imas_pm_grkhxx b where a" +
                        ".data_date = b.sjrq and a.`客户号` = b.khh and b.sjrq = '"+now+"')";
        lstNow = jdbcTemplate.queryForList(sql);
        processGRKHXX(now, lstNow, group_id);
        sql = "select * from ODS_GRKHXX where data_date = '" + now + "'";
        lstNow = jdbcTemplate.queryForList(sql);
        processOtherInfo(now, lstNow, group_id);
        return true;
    }

    public boolean processAll(String now, String group_id) throws Exception {
        logger.info(">>>Start GRKHXX " + now + " " + group_id);
        String sql = String.format("delete from imas_pm_grkhxx where sjrq = '%s'", now);
        execUpdSqlCommit(sql);
        sql = "select * from ODS_BOSC_GRKHXX where data_date = '" + now + "'";
        List<Map<String, Object>> lstBase = jdbcTemplate.queryForList(sql);
        sql = "select * from ODS_GRKHXX where data_date = '" + now + "'";
        List<Map<String, Object>> lstOther = jdbcTemplate.queryForList(sql);
        processNew(now, lstBase, lstOther, group_id);
        return true;
    }

    public void processNew(String now, List<Map<String, Object>> lstBase, List<Map<String, Object>> lstOther,
                           String groupId) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstBase) {
            base.add(addGRKHXXBASE(now, record, groupId));
        }
        for (Map<String, Object> record : lstOther) {
            String khh = getString(record.get("客户号"));
            boolean find = false;
            for (int i = 0; i < base.size(); i++) {
                if (base.get(i).get(1).equals(khh)) {
                    String nbjgh = getString(record.get("内部机构号"));
                    if (!nbjgh.equals("")) {
                        nbjgh = formatNBJGH(nbjgh);
                        base.get(i).set(2, nbjgh);
                    }
                    String CZDHZQHDM = getString(record.get("常住地行政区划代码"));
                    if (!CZDHZQHDM.equals("")) {
                        base.get(i).set(3, CZDHZQHDM);
                    }
                    String SXED = getString(record.get("授信额度"));
                    if (!SXED.equals("")) {
                        String oSXED = base.get(i).get(4);
                        if (oSXED.equals("")) {
                            oSXED = "0";
                        }
                        base.get(i).set(4, new BigDecimal(oSXED).add(new BigDecimal(SXED)).toString());
                    }
                    String YYED = getString(record.get("已用额度"));
                    if (!YYED.equals("")) {
                        String oYYED = base.get(i).get(5);
                        if (oYYED.equals("")) {
                            oYYED = "0";
                        }
                        base.get(i).set(5, new BigDecimal(oYYED).add(new BigDecimal(YYED)).toString());
                    }
                    String KHXL = getString(record.get("客户细类"));
                    if (!KHXL.equals("")) {
                        base.get(i).set(6, KHXL);
                    }
                    String NHBZ = getString(record.get("农户标志"));
                    if (!NHBZ.equals("")) {
                        base.get(i).set(7, NHBZ);
                    }
                    base.get(i).set(8, getString(record.get("GROUP_ID")));
                    find = true;
                    break;
                }
            }
            if (!find) {
                base.add(addGRKHXX(now, record, groupId));
            }
        }

        /*
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstOther) {
            String khh = getString(record.get("客户号"));
            boolean find = false;
            for (int i = 0; i < base.size(); i++) {
                if (base.get(i).get(1).equals(khh)) {
                    String nbjgh = getString(record.get("内部机构号"));
                    if (!nbjgh.equals("")) {
                        nbjgh = formatNBJGH(nbjgh);
                        base.get(i).set(2, nbjgh);
                    }
                    String CZDHZQHDM = getString(record.get("常住地行政区划代码"));
                    if (!CZDHZQHDM.equals("")) {
                        base.get(i).set(3, CZDHZQHDM);
                    }
                    String SXED = getString(record.get("授信额度"));
                    if (!SXED.equals("")) {
                        String oSXED = base.get(i).get(4);
                        if (oSXED.equals("")) {
                            oSXED = "0";
                        }
                        base.get(i).set(4, new BigDecimal(oSXED).add(new BigDecimal(SXED)).toString());
                    }
                    String YYED = getString(record.get("已用额度"));
                    if (!YYED.equals("")) {
                        String oYYED = base.get(i).get(5);
                        if (oYYED.equals("")) {
                            oYYED = "0";
                        }
                        base.get(i).set(5, new BigDecimal(oYYED).add(new BigDecimal(YYED)).toString());
                    }
                    String KHXL = getString(record.get("客户细类"));
                    if (!KHXL.equals("")) {
                        base.get(i).set(6, KHXL);
                    }
                    String NHBZ = getString(record.get("农户标志"));
                    if (!NHBZ.equals("")) {
                        base.get(i).set(7, NHBZ);
                    }
                    base.get(i).set(8, getString(record.get("GROUP_ID")));
                    find = true;
                    break;
                }
            }
            if (!find) {
                base.add(addGRKHXX(now, record, groupId));
            }
        }
        int cnt = base.size();
        for (Map<String, Object> record : lstBase) {
            boolean find = false;
            String khh = formatKHH(record.get("客户号")+"-"+record.get("客户号1"));
            for (int i = 0; i < cnt; i++) {
                if (base.get(i).get(1).equals(khh)) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                base.add(addGRKHXXBASE(now, record, groupId));
            }
        }
         */

        insertService.insertData(SQL_GRKHXX, groupId, groupId, base);
    }

    private void execUpdSqlCommit(String sql) {
        transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
            jdbcTemplate.update(sql);
        });
    }
}
