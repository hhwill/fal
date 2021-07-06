package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlConst.*;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlWPB {

    private final Logger logger = LoggerFactory.getLogger(CustEtlWPB.class);

    private final EtlInsertService insertService;

    private final JdbcTemplate jdbcTemplate;

    private final DataSource dataSource;

    private int threadSize;

    private int pageSize;

    public CustEtlWPB(EtlInsertService insertService, DataSource dataSource) {
        this.insertService = insertService;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.dataSource = dataSource;
        threadSize = 10;
        pageSize = 1000;
    }

    private boolean processdd(String now, String group_id) {
        ExecutorService execservice = Executors.newFixedThreadPool(threadSize);
        String sql = "select * from ods_wpb_dd where data_date = '"+now+"' and group_id = '"+group_id+"'";
        CompletionService<List<List<List<String>>>> completionService = new ExecutorCompletionService<List<List<List<String>>>>(execservice);
        String querySql = "SELECT COUNT(1) FROM (" + sql + ")";
        Integer totalCount = jdbcTemplate.queryForObject(querySql, Integer.class);
        int times = totalCount / pageSize;
        if (totalCount % pageSize != 0) {
            times += 1;
        }
        int bindex = 0;
        List<Callable<List<List<List<String>>>>> tasks = new ArrayList<Callable<List<List<List<String>>>>>();
        for (int i = 0; i < times; i++) {
            Callable<List<List<List<String>>>> qfe = new CustEtlWPBDDThread(dataSource, sql,
                    pageSize, bindex, now);
            tasks.add(qfe);
            bindex++;
        }
        try {
            for (int i = 0; i < tasks.size(); i++) {
                completionService.submit(tasks.get(i));
            }
            for (int i = 0; i < tasks.size(); i++) {
                List<List<List<String>>> datas = completionService.take().get();
                insertService.insertData(SQL_GRCKJC, group_id, group_id, datas.get(0));
                insertService.insertData(SQL_GRCKYE, group_id, group_id, datas.get(1));
            }
        } catch (Exception ex) {
            logger.error("ods2pm failed", ex);
        }
        return true;
    }
    private boolean processtd(String now, String group_id) {
        ExecutorService execservice = Executors.newFixedThreadPool(threadSize);
        String sql = "select * from ods_wpb_td where data_date = '"+now+"' and group_id = '"+group_id+"'";
        CompletionService<List<List<List<String>>>> completionService = new ExecutorCompletionService<List<List<List<String>>>>(execservice);
        String querySql = "SELECT COUNT(1) FROM (" + sql + ")";
        Integer totalCount = jdbcTemplate.queryForObject(querySql, Integer.class);
        int times = totalCount / pageSize;
        if (totalCount % pageSize != 0) {
            times += 1;
        }
        int bindex = 0;
        List<Callable<List<List<List<String>>>>> tasks = new ArrayList<Callable<List<List<List<String>>>>>();
        for (int i = 0; i < times; i++) {
            Callable<List<List<List<String>>>> qfe = new CustEtlWPBTDThread(dataSource, sql,
                    pageSize, bindex, now);
            tasks.add(qfe);
            bindex++;
        }
        try {
            for (int i = 0; i < tasks.size(); i++) {
                completionService.submit(tasks.get(i));
            }
            for (int i = 0; i < tasks.size(); i++) {
                List<List<List<String>>> datas = completionService.take().get();
                insertService.insertData(SQL_GRCKJC, group_id, group_id, datas.get(0));
                insertService.insertData(SQL_GRCKYE, group_id, group_id, datas.get(1));
                insertService.insertData(SQL_GRCKFS, group_id, group_id, datas.get(2));
            }
        } catch (Exception ex) {
            logger.error("ods2pm failed", ex);
        }
        return true;
    }

    private boolean processfs(String now, String group_id) {
        String sql = "select max(data_date) from ods_wpb_dd where data_date < '" + now + "'";
        String previous = jdbcTemplate.queryForObject(sql, String.class);
        sql = String.format("select a.*, b.ledger as OLDLEDGER from ( select * from ods_wpb_dd where data_date"
                        + " = '%s') a inner join (select * from ods_wpb_dd where data_date = '%s') b "
                        + " on a.dfacb=b.dfacb and a.dfacs=b.dfacs and a.dfacx = b.dfacx and a.ledger <> b.ledger"
                        + " union "
                        + " select a.*, 0 as OLDLEDGER from (select * from ods_wpb_bb where data_date"
                        + " = '%s') a where not exists (select * from (select dfacb,dfacs,dfacx from ods_wpb_dd "
                        + " where data_date = '%s') b where a.dfacb=b.dfacb and a.dfacs=b.dfacs and a.dfacx = b.dfacx",
                now, previous, now, previous);
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        List<List<String>> grckfs = new ArrayList<List<String>>();
        for (Map<String, Object> src : lst) {
            List<List<String>> ckxx = getCKXH(src);
            List<String> subgrckfs = new ArrayList<String>();

            subgrckfs.add(now);
            subgrckfs.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
            subgrckfs.add("01");
            subgrckfs.add(formatNBJGH(src.get("DFDCB")));
            subgrckfs.add(getString(src.get("CUS")));
            //TODO 交易流水号
            String DPDLNO = getString(src.get("DPDLNO"));
            while (DPDLNO.length() < 5) {
                DPDLNO = "0" + DPDLNO;
            }
            subgrckfs.add(now+formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX"))+DPDLNO);
            subgrckfs.add(getString(src.get("DPCPDT")));
            subgrckfs.add("");
            //实际利率
            subgrckfs.add(ckxx.get(0).get(0));
            String ccy = getString(src.get("DFCYCD"));
            subgrckfs.add(ccy);
            String TRANAMT = new BigDecimal(getString(src.get("LEDGER"))).subtract(new BigDecimal(getString(src.get(
                    "LEDGER")))).toString();
            String ABSTRANAMT = TRANAMT;
            if (ABSTRANAMT.startsWith("-")) {
                ABSTRANAMT = ABSTRANAMT.substring(1);
            }
            subgrckfs.add(ABSTRANAMT);
            //交易渠道
            subgrckfs.add("01");

            if (new BigDecimal(TRANAMT).compareTo(new BigDecimal("0")) > 0) {
                subgrckfs.add("1");
            } else {
                subgrckfs.add("0");
            }
            if (ccy.equals("CNY")) {
                subgrckfs.add("");
            } else {
                //usd >=300W then A else B
                if (ccy.equals("EUR") || ccy.equals("HKD") || ccy.equals("JPY")) {
                    String currate = getMap("RATE", ccy +"/USD");
                    if (!currate.equals("")) {
                        BigDecimal x = new BigDecimal(getString(src.get("TRANAMT"))).multiply(new BigDecimal(currate));
                        if (x.compareTo(new BigDecimal("3000000")) > -1) {
                            subgrckfs.add("A");
                        } else {
                            subgrckfs.add("B");
                        }
                    } else {
                        subgrckfs.add("");
                    }
                } else {
                    subgrckfs.add("");
                }
            }
            grckfs.add(subgrckfs);
        }
        insertService.insertData(SQL_GRCKFS, group_id, group_id, grckfs);
        return true;
    }


    public boolean process(String now, String group_id) {
        processdd(now, group_id);
        processtd(now, group_id);
        processfs(now, group_id);
        return true;
    }

    public void test() {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<String> r = new ArrayList<String>();
        r.add("20210531");
        r.add("20210531");
        r.add("01");
        r.add("20210531");
        r.add("20210531");
        r.add("D011");
        r.add("");
        r.add("20210531");
        r.add("");
        r.add("");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        r.add("1");
        dwckjc.add(r);
        insertService.insertData(SQL_DWCKJC, "a", "a", dwckjc);
    }
}
