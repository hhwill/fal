package com.gingkoo.imas.hsbc.service;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import com.gingkoo.root.facility.spring.tx.TransactionHelper;
import static com.gingkoo.imas.hsbc.service.EtlConst.*;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlGM {
    private final Logger logger = LoggerFactory.getLogger(CustEtlGM.class);

    private final EtlInsertService insertService;

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    private final DataSource dataSource;

    private final CustLoadFileProcessService pService;

    public CustEtlGM(EtlInsertService insertService, TransactionHelper transactionTemplate,
                     CustLoadFileProcessService pService, DataSource dataSource) {
        this.insertService = insertService;
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.pService = pService;
        this.dataSource = dataSource;
    }

    public void initPCBase(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYCKJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_TYJDJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_MRFSJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyckjc = new ArrayList<String>();
            subtyckjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dealing = getCellValue(row.getCell(14));
            String cbclass = getCellValue(row.getCell(15));
            String numberofdays = getCellValue(row.getCell(16));
            if (numberofdays.equals("")) {
                numberofdays = "0";
            }
            if (numberofdays.endsWith(".00")) {
                numberofdays = numberofdays.substring(0, numberofdays.length()-3);
            }
            subtyckjc.add(formatGM_NBJGH(dealing));

            if (khh.contains("-")) {
               subtyckjc.add(formatKHH(khh));
            } else {
                subtyckjc.add("");
            }
            //jrjglxdm
            subtyckjc.add(getMap("X42", cbclass).trim());
            subtyckjc.add(getCellValue(row.getCell(4)));
            subtyckjc.add(getCellValue(row.getCell(5)));
            subtyckjc.add(getCellValue(row.getCell(6)));
            subtyckjc.add(getCellValue(row.getCell(7)));
            //存款期限类型s
            subtyckjc.add(checkTyjdTenor(numberofdays, map.get("X14")));
            subtyckjc.add(getCellValue(row.getCell(9)));
            subtyckjc.add(getCellValue(row.getCell(10)));
            subtyckjc.add(getCellValue(row.getCell(11)));
            subtyckjc.add(getCellValue(row.getCell(12)));
            subtyckjc.add(getCellValue(row.getCell(13)));

            logger.info(">>><<<<" + subtyckjc.toString());
            tyckjc.add(subtyckjc);
        }
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        st = wb.getSheetAt(1);
        List<List<String>> tyjdjc = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyjdjc = new ArrayList<String>();
            subtyjdjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dealing = getCellValue(row.getCell(16));
            String cbclass = getCellValue(row.getCell(17));
            String numberofdays = getCellValue(row.getCell(18));
            if (numberofdays.equals("")) {
                numberofdays = "0";
            }
            if (numberofdays.endsWith(".00")) {
                numberofdays = numberofdays.substring(0, numberofdays.length()-3);
            }
            subtyjdjc.add(formatGM_NBJGH(dealing));

            if (khh.contains("-")) {
                subtyjdjc.add(formatKHH(khh));
            } else {
                subtyjdjc.add("");
            }
            //jrjglxdm
            subtyjdjc.add(getMap("X42", cbclass).trim());
            subtyjdjc.add(getCellValue(row.getCell(4)));
            subtyjdjc.add(getCellValue(row.getCell(5)));
            subtyjdjc.add(getCellValue(row.getCell(6)));
            subtyjdjc.add(getCellValue(row.getCell(7)));
            subtyjdjc.add(getCellValue(row.getCell(8)));
            subtyjdjc.add(checkTyjdTenor(numberofdays, map.get("X14")));
            subtyjdjc.add(getCellValue(row.getCell(10)));
            subtyjdjc.add(getCellValue(row.getCell(11)));
            subtyjdjc.add(getCellValue(row.getCell(12)));
            subtyjdjc.add(getCellValue(row.getCell(13)));
            subtyjdjc.add(getCellValue(row.getCell(14)));
            subtyjdjc.add(getCellValue(row.getCell(15)));

            tyjdjc.add(subtyjdjc);
        }
        insertService.insertData(SQL_TYJDJC, group_id, group_id, tyjdjc);
        st = wb.getSheetAt(2);
        List<List<String>> mrfsjc = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> submrfsjc = new ArrayList<String>();
            submrfsjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dealing = getCellValue(row.getCell(17));
            String cbclass = getCellValue(row.getCell(18));
            String numberofdays = getCellValue(row.getCell(19));
            if (numberofdays.equals("")) {
                numberofdays = "0";
            }
            if (numberofdays.endsWith(".00")) {
                numberofdays = numberofdays.substring(0, numberofdays.length()-3);
            }
            submrfsjc.add(formatGM_NBJGH(dealing));

            if (khh.contains("-")) {
                submrfsjc.add(formatKHH(khh));
            } else {
                submrfsjc.add("");
            }
            //jrjglxdm
            submrfsjc.add(getMap("X42", cbclass).trim());
            submrfsjc.add(getCellValue(row.getCell(4)));
            submrfsjc.add(getCellValue(row.getCell(5)));
            submrfsjc.add(getCellValue(row.getCell(6)));
            submrfsjc.add(getCellValue(row.getCell(7)));
            submrfsjc.add(getCellValue(row.getCell(8)));
            submrfsjc.add(getCellValue(row.getCell(9)));
            submrfsjc.add(checkTyjdTenor(numberofdays, map.get("X14")));
            submrfsjc.add(getCellValue(row.getCell(11)));
            submrfsjc.add(getCellValue(row.getCell(12)));
            submrfsjc.add(getCellValue(row.getCell(13)));
            submrfsjc.add(getCellValue(row.getCell(14)));
            submrfsjc.add(getCellValue(row.getCell(15)));
            submrfsjc.add(getCellValue(row.getCell(16)));

            mrfsjc.add(submrfsjc);
        }
        insertService.insertData(SQL_MRFSJC, group_id, group_id, mrfsjc);
    }

    public void initBalance(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYCKYE where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_TYJDYE where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_MRFSYE where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        execUpdSqlCommit(sql);
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyckye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyckye = new ArrayList<String>();
            subtyckye.add(now);
            subtyckye.add(getCellValue(row.getCell(1)));
            String nbjgh = getCellValue(row.getCell(2));
            subtyckye.add(formatGM_NBJGH(nbjgh));
            String khh = getCellValue(row.getCell(3));
            if (khh.contains("-")) {
                subtyckye.add(formatKHH(khh));
            } else {
                subtyckye.add("");
            }
            subtyckye.add(getCellValue(row.getCell(4)));
            subtyckye.add(getCellValue(row.getCell(5)));
            tyckye.add(subtyckye);
        }
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
        st = wb.getSheetAt(1);
        List<List<String>> tyjdye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyjdye = new ArrayList<String>();
            subtyjdye.add(now);
            subtyjdye.add(getCellValue(row.getCell(1)));
            String nbjgh = getCellValue(row.getCell(2));
            subtyjdye.add(formatGM_NBJGH(nbjgh));
            String khh = getCellValue(row.getCell(3));
            if (khh.contains("-")) {
                subtyjdye.add(formatKHH(khh));
            } else {
                subtyjdye.add("");
            }
            subtyjdye.add(getCellValue(row.getCell(4)));
            subtyjdye.add(getCellValue(row.getCell(5)));
            tyjdye.add(subtyjdye);
        }
        insertService.insertData(SQL_TYJDYE, group_id, group_id, tyjdye);
        st = wb.getSheetAt(2);
        List<List<String>> mrfsye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> submrfsye = new ArrayList<String>();
            submrfsye.add(now);
            submrfsye.add(getCellValue(row.getCell(1)));
            String nbjgh = getCellValue(row.getCell(2));
            submrfsye.add(formatGM_NBJGH(nbjgh));
            String khh = getCellValue(row.getCell(3));
            if (khh.contains("-")) {
                submrfsye.add(formatKHH(khh));
            } else {
                submrfsye.add("");
            }
            submrfsye.add(getCellValue(row.getCell(4)));
            submrfsye.add(getCellValue(row.getCell(5)));
            mrfsye.add(submrfsye);
        }
        insertService.insertData(SQL_MRFSYE, group_id, group_id, mrfsye);
    }

    public void initJC(String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYCKJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        jdbcTemplate.execute(sql);
        sql = "delete from imas_pm_TYJDJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        jdbcTemplate.execute(sql);
        sql = "delete from imas_pm_MRFSJC where sjrq = '"+now+"' and group_id = '"+group_id+"'";
        jdbcTemplate.execute(sql);
        sql = "select max(sjrq) from imas_pm_jgfrxx where data_date < '" + now + "'";
        String previous = jdbcTemplate.queryForObject(sql, String.class);
        sql = String.format("insert into imas_pm_TYCKJC select * from imas_pm_TYCKJC a where sjrq = '%s' and rsv5 <> " +
                "'0' and not exists (select * from imas_pm_TYCKJC b where b.sjrq = '%s' and a.ckzhbm = b.ckzhbm and a" +
                ".sjrq = '%s')", previous, now, previous);
        jdbcTemplate.execute(sql);
        sql = String.format("insert into imas_pm_TYJDJC select * from imas_pm_TYJDJC a where sjrq = '%s' and rsv5 <> " +
                "'0' and not exists (select * from imas_pm_TYJDJC b where b.sjrq = '%s' and a.ywbm = b.ywbm and a" +
                ".sjrq = '%s')", previous, now, previous);
        jdbcTemplate.execute(sql);
        sql = String.format("insert into imas_pm_MRFSJC select * from imas_pm_MRFSJC a where sjrq = '%s' and rsv5 <> " +
                "'0' and not exists (select * from imas_pm_MRFSC b where b.sjrq = '%s' and a.ywbm = b.ywbm and a" +
                ".sjrq = '%s')", previous, now, previous);
        jdbcTemplate.execute(sql);
    }

    public void initGMOTYCKJC(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYCKJC where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        List<Map<String, Object>> lst =
                jdbcTemplate.queryForList("select * from imas_pm_TYCKJC where sjrq = '" + now + "'");
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckjcupd = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyckjc = new ArrayList<String>();
            List<String> subtyckjcupd = new ArrayList<String>();
            subtyckjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dlno = getCellValue(row.getCell(14));
            String cpac = getCellValue(row.getCell(15));
            String bkcs = getCellValue(row.getCell(16));
            String dpos = getCellValue(row.getCell(17));
            //0-转出 结清 收回 1-转入 发生
            String jyfx = getCellValue(row.getCell(18));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String nbjgh = "";
            if (khh.length() > 9) {
                nbjgh = khh.substring(0,9);
            }
            subtyckjc.add(nbjgh);

            if (!khh.equals("")) {
                subtyckjc.add(formatKHH(khh));
            } else {
                subtyckjc.add("");
            }
            //jrjglxdm
            subtyckjc.add(getMap("X42", bkcs).trim());
            String YWBM = getCellValue(row.getCell(4));
            subtyckjc.add(YWBM);
            subtyckjc.add(getCellValue(row.getCell(5)));
            subtyckjc.add(getCellValue(row.getCell(6)).replace("-",""));
            subtyckjc.add(getCellValue(row.getCell(7)).replace("-",""));
            //存款期限类型s
            subtyckjc.add(checkTyjdTenor(getCellValue(row.getCell(8)), map.get("X14")));
            subtyckjc.add(getCellValue(row.getCell(9)));
            subtyckjc.add(getCellValue(row.getCell(10)));
            subtyckjc.add(getCellValue(row.getCell(11)));
            subtyckjc.add(getCellValue(row.getCell(12)));
            subtyckjc.add(getCellValue(row.getCell(13)));
            subtyckjc.add(jyfx);
            boolean find = false;
            for (Map<String, Object> record : lst) {
                if (record.get("KHH").equals(khh) && record.get("YWBM").equals(YWBM)) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                logger.info(">>><<<<" + subtyckjc.toString());
                tyckjc.add(subtyckjc);
            } else {
                subtyckjcupd.add(jyfx);
                subtyckjcupd.add(YWBM);
                subtyckjcupd.add(now);
                tyckjcupd.add(subtyckjcupd);
            }
        }
        insertService.insertData(SQL_TYCKJC_GMO, group_id, group_id, tyckjc);
        insertService.updateData(SQL_TYCKJC_UPDATE, now, tyckjcupd);
    }

    public void initGMOTYJDJC(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYJDJC where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        List<Map<String, Object>> lst =
                jdbcTemplate.queryForList("select * from imas_pm_TYJDJC where sjrq = '" + now + "'");
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyjdjc = new ArrayList<List<String>>();
        List<List<String>> tyjdjcupd = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyjdjc = new ArrayList<String>();
            List<String> subtyjdjcupd = new ArrayList<String>();
            subtyjdjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dlno = getCellValue(row.getCell(16));
            String cpac = getCellValue(row.getCell(17));
            String bkcs = getCellValue(row.getCell(18));
            String dpos = getCellValue(row.getCell(19));
            String jyfx = getCellValue(row.getCell(20));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String nbjgh = "";
            if (khh.length() > 9) {
                nbjgh = khh.substring(0,9);
            }
            subtyjdjc.add(nbjgh);

            if (!khh.equals("")) {
                subtyjdjc.add(formatKHH(khh));
            } else {
                subtyjdjc.add("");
            }
            //jrjglxdm
            subtyjdjc.add(getMap("X42", bkcs).trim());
            String YWBM = getCellValue(row.getCell(4));
            subtyjdjc.add(YWBM);
            subtyjdjc.add(getCellValue(row.getCell(5)));
            subtyjdjc.add(getCellValue(row.getCell(6)).replace("-",""));
            subtyjdjc.add(getCellValue(row.getCell(7)).replace("-",""));
            subtyjdjc.add(getCellValue(row.getCell(8)));
            //存款期限类型s
            subtyjdjc.add(checkTyjdTenor(getCellValue(row.getCell(9)), map.get("X14")));

            subtyjdjc.add(getCellValue(row.getCell(10)));
            subtyjdjc.add(getCellValue(row.getCell(11)));
            subtyjdjc.add(getCellValue(row.getCell(12)));
            subtyjdjc.add(getCellValue(row.getCell(13)));
            subtyjdjc.add(getCellValue(row.getCell(14)));
            subtyjdjc.add(getCellValue(row.getCell(15)));
            subtyjdjc.add(jyfx);
            boolean find = false;
            for (Map<String, Object> record : lst) {
                if (record.get("KHH").equals(khh) && record.get("YWBM").equals(YWBM)) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                logger.info(">>><<<<" + subtyjdjc.toString());
                tyjdjc.add(subtyjdjc);
            } else {
                subtyjdjcupd.add(jyfx);
                subtyjdjcupd.add(YWBM);
                subtyjdjcupd.add(now);
                tyjdjcupd.add(subtyjdjcupd);
            }
        }
        insertService.insertData(SQL_TYJDJC_GMO, group_id, group_id, tyjdjc);
        insertService.updateData(SQL_TYJDJC_UPDATE, now, tyjdjcupd);
    }

    public void initGMOMRFSJC(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_MRFSJC where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        List<Map<String, Object>> lst =
                jdbcTemplate.queryForList("select * from imas_pm_MRFSJC where sjrq = '" + now + "'");
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> mrfsjc = new ArrayList<List<String>>();
        List<List<String>> mrfsjcupd = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> submrfsjc = new ArrayList<String>();
            List<String> submrfsjcupd = new ArrayList<String>();
            submrfsjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dlno = getCellValue(row.getCell(17));
            String cpac = getCellValue(row.getCell(18));
            String bkcs = getCellValue(row.getCell(19));
            String dpos = getCellValue(row.getCell(20));
            String jyfx = getCellValue(row.getCell(21));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String nbjgh = "";
            if (khh.length() > 9) {
                nbjgh = khh.substring(0,9);
            }
            submrfsjc.add(nbjgh);

            if (!khh.equals("")) {
                submrfsjc.add(formatKHH(khh));
            } else {
                submrfsjc.add("");
            }
            //jrjglxdm
            submrfsjc.add(getMap("X42", bkcs).trim());
            String YWBM = getCellValue(row.getCell(4));
            submrfsjc.add(YWBM);
            submrfsjc.add(getCellValue(row.getCell(5)));
            submrfsjc.add(getCellValue(row.getCell(6)));
            submrfsjc.add(getCellValue(row.getCell(7)).replace("-",""));
            submrfsjc.add(getCellValue(row.getCell(8)).replace("-",""));
            submrfsjc.add(getCellValue(row.getCell(9)).replace("-",""));
            submrfsjc.add(checkTyjdTenor(getCellValue(row.getCell(10)), map.get("X14")));
            submrfsjc.add(getCellValue(row.getCell(11)));
            submrfsjc.add(getCellValue(row.getCell(12)));
            submrfsjc.add(getCellValue(row.getCell(13)));
            submrfsjc.add(getCellValue(row.getCell(14)));
            submrfsjc.add(getCellValue(row.getCell(15)));
            submrfsjc.add(getCellValue(row.getCell(16)));
            submrfsjc.add(jyfx);
            boolean find = false;
            for (Map<String, Object> record : lst) {
                if (record.get("KHH").equals(khh) && record.get("YWBM").equals(YWBM)) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                logger.info(">>><<<<" + submrfsjc.toString());
                mrfsjc.add(submrfsjc);
            } else {
                submrfsjcupd.add(jyfx);
                submrfsjcupd.add(YWBM);
                submrfsjcupd.add(now);
                mrfsjcupd.add(submrfsjcupd);
            }
        }
        insertService.insertData(SQL_MRFSJC_GMO, group_id, group_id, mrfsjc);
        insertService.updateData(SQL_MRFSJC_UPDATE, now, mrfsjcupd);
    }

    public void initGMOTYCKFS(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYCKFS where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_TYCKYE where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "select * from imas_pm_TYCKYE where sjrq = '" + now + "'";
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyckfs = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyckfs = new ArrayList<String>();
            subtyckfs.add(now);
            String khh = getCellValue(row.getCell(3));
            String dlno = getCellValue(row.getCell(11));
            String cpac = getCellValue(row.getCell(12));
            String bkcs = getCellValue(row.getCell(13));
            String dpos = getCellValue(row.getCell(14));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String ckzhbm = getCellValue(row.getCell(1));
            subtyckfs.add(ckzhbm);
            String nbjgh = formatGM_NBJGH(dpos);
            subtyckfs.add(nbjgh);

            if (!khh.equals("")) {
                khh = formatKHH(khh);
            }
            subtyckfs.add(khh);
            subtyckfs.add(getCellValue(row.getCell(4)));
            subtyckfs.add(getCellValue(row.getCell(5)).replace("-",""));
            subtyckfs.add(getCellValue(row.getCell(6)));
            subtyckfs.add(getCellValue(row.getCell(7)));
            subtyckfs.add(getCellValue(row.getCell(8)));
            subtyckfs.add(getCellValue(row.getCell(9)));
            String jyfx = getCellValue(row.getCell(10));
            subtyckfs.add(jyfx);
            logger.info(">>><<<<" + subtyckfs.toString());
            tyckfs.add(subtyckfs);
            if (jyfx.equals("0")) {
                boolean find = false;
                for (Map<String,Object> record: lst) {
                    if (record.get("CKZHBM").equals(ckzhbm)) {
                        find = true;
                        break;
                    }
                }
                if (!find) {
                    List<String> subtyckye = new ArrayList<String>();
                    subtyckye.add(now);
                    subtyckye.add(ckzhbm);
                    subtyckye.add(nbjgh);
                    subtyckye.add(khh);
                    subtyckye.add(getCellValue(row.getCell(6)));
                    subtyckye.add("0");
                    tyckye.add(subtyckye);
                }
            }
        }
        insertService.insertData(SQL_TYCKFS, group_id, group_id, tyckfs);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
    }

    public void initGMOTYJDFS(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_TYJDFS where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_TYJDYE where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "select * from imas_pm_TYJDYE where sjrq = '" + now + "'";
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> tyjdfs = new ArrayList<List<String>>();
        List<List<String>> tyjdye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> subtyjdjc = new ArrayList<String>();
            subtyjdjc.add(now);
            String khh = getCellValue(row.getCell(2));
            String dlno = getCellValue(row.getCell(11));
            String cpac = getCellValue(row.getCell(12));
            String bkcs = getCellValue(row.getCell(13));
            String dpos = getCellValue(row.getCell(14));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String ywbm = getCellValue(row.getCell(1));
            subtyjdjc.add(ywbm);
            String nbjgh = formatGM_NBJGH(dpos);
            subtyjdjc.add(nbjgh);

            if (!khh.equals("")) {
                khh = formatKHH(khh);
            }
            subtyjdjc.add(khh);
            subtyjdjc.add(getCellValue(row.getCell(4)));
            subtyjdjc.add(getCellValue(row.getCell(5)));
            subtyjdjc.add(getCellValue(row.getCell(6)));
            subtyjdjc.add(getCellValue(row.getCell(7)));
            subtyjdjc.add(getCellValue(row.getCell(8)));
            subtyjdjc.add(getCellValue(row.getCell(9)));
            String jyfx = getCellValue(row.getCell(10));
            subtyjdjc.add(jyfx);
            logger.info(">>><<<<" + subtyjdjc.toString());
            tyjdfs.add(subtyjdjc);
            if (jyfx.equals("0")) {
                boolean find = false;
                for (Map<String,Object> record: lst) {
                    if (record.get("CKZHBM").equals(ywbm)) {
                        find = true;
                        break;
                    }
                }
                if (!find) {
                    List<String> subtyjdye = new ArrayList<String>();
                    subtyjdye.add(now);
                    subtyjdye.add(ywbm);
                    subtyjdye.add(nbjgh);
                    subtyjdye.add(khh);
                    subtyjdye.add(getCellValue(row.getCell(6)));
                    subtyjdye.add("0");
                    tyjdye.add(subtyjdye);
                }
            }
        }
        insertService.insertData(SQL_TYJDFS, group_id, group_id, tyjdfs);
        insertService.insertData(SQL_TYJDYE, group_id, group_id, tyjdye);
    }

    public void initGMOMRFSFS(String dir, String now, String group_id) throws Exception {
        pService.initMap(now);
        String sql = "delete from imas_pm_MRFSFS where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "delete from imas_pm_MRFSYE where sjrq = '" + now + "' and group_id = '" + group_id + "'";
        execUpdSqlCommit(sql);
        sql = "select * from imas_pm_MRFSYE where sjrq = '" + now + "'";
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        List<List<String>> mrfsfs = new ArrayList<List<String>>();
        List<List<String>> mrfsye = new ArrayList<List<String>>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            String sjrq = getCellValue(row.getCell(0));
            if (sjrq.length() < 2) {
                continue;
            }
            List<String> submrfsfs = new ArrayList<String>();
            submrfsfs.add(now);
            String khh = getCellValue(row.getCell(2));
            String dlno = getCellValue(row.getCell(17));
            String cpac = getCellValue(row.getCell(18));
            String bkcs = getCellValue(row.getCell(19));
            String dpos = getCellValue(row.getCell(20));
            if (khh.equals("")) {
                khh = getGMOKHH(cpac);
            }
            String ywbm = getCellValue(row.getCell(1));
            submrfsfs.add(ywbm);
            String nbjgh = formatGM_NBJGH(dpos);
            submrfsfs.add(nbjgh);

            if (!khh.equals("")) {
                khh = formatKHH(khh);
            }
            submrfsfs.add(khh);
            submrfsfs.add(getCellValue(row.getCell(4)));
            submrfsfs.add(getCellValue(row.getCell(5)));
            submrfsfs.add(getCellValue(row.getCell(6)));
            submrfsfs.add(getCellValue(row.getCell(7)));
            submrfsfs.add(getCellValue(row.getCell(8)));
            submrfsfs.add(getCellValue(row.getCell(9)));
            String jyfx = getCellValue(row.getCell(10));
            submrfsfs.add(jyfx);
            mrfsfs.add(submrfsfs);
            if (jyfx.equals("0")) {
                boolean find = false;
                for (Map<String,Object> record: lst) {
                    if (record.get("CKZHBM").equals(ywbm)) {
                        find = true;
                        break;
                    }
                }
                if (!find) {
                    List<String> submrfsye = new ArrayList<String>();
                    submrfsye.add(now);
                    submrfsye.add(ywbm);
                    submrfsye.add(nbjgh);
                    submrfsye.add(khh);
                    submrfsye.add(getCellValue(row.getCell(6)));
                    submrfsye.add("0");
                    mrfsye.add(submrfsye);
                }
            }
        }
        insertService.insertData(SQL_MRFSFS, group_id, group_id, mrfsfs);
        insertService.insertData(SQL_MRFSYE, group_id, group_id, mrfsye);
    }

    private void execUpdSqlCommit(String sql) {
        transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
            jdbcTemplate.update(sql);
        });
    }
}

