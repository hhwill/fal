package com.gingkoo.imas.hsbc.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import javax.sql.DataSource;

import com.monitorjbl.xlsx.StreamingReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.pentaho.di.core.util.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import com.gingkoo.gf4j2.framework.service.SysParamService;
import com.gingkoo.imas.core.batch.ImasBatchBasicValidateService;
import com.gingkoo.root.facility.spring.tx.TransactionHelper;
import com.gingkoo.root.facility.string.UuidHelper;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
@Component
public class CustLoadFileService {

    private final Logger logger = LoggerFactory.getLogger(HsbcAutoProcess.class);

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    private ImasBatchBasicValidateService imasBatchBasicValidateService;

    private CustLoadFileProcessService custLoadFileProcessService;

    private CustEtlGM etlGM;

    public CustLoadFileService(TransactionHelper transactionTemplate, DataSource dataSource,CustEtlGM etlGM,
                               ImasBatchBasicValidateService imasBatchBasicValidateService, CustLoadFileProcessService custLoadFileProcessService) {
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.etlGM = etlGM;
        this.imasBatchBasicValidateService = imasBatchBasicValidateService;
        this.custLoadFileProcessService = custLoadFileProcessService;
    }

    @Value("${application.home}")
    private String filePath;

    public void run() {
        log.info("CustLoadFileService");
        newProcessFiles(filePath);
//        List<String> lst = new ArrayList<String>();
//        lst.add("PJTXFS");
//        lst.add("PJTXJC");
//        lst.add("PJTXYE");
//        try {
//            imasBatchBasicValidateService.validate("20210531", lst, null, null, null, true);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
    }

    private void newProcessFiles(String backupdir) {
        String sql = "select * from gp_bm_id_uploadlog where filler1 = '未导入' order by upload_time";
        List<Map<String, Object>> files = jdbcTemplate.queryForList(sql);
        sql = "select * from ods_ctl order by order_no";
        List<Map<String, Object>> records = jdbcTemplate.queryForList("select * from ods_ctl order by order_no");

        sql = "select * from map_groupid";
        List<Map<String, Object>> groupids = jdbcTemplate.queryForList(sql);
        Map<String, String> groupidmap = new HashMap<String, String>();
        for (Map<String, Object> key : groupids) {
            groupidmap.put(key.get("SRC").toString(), key.get("DEST").toString());
        }
        for (Map<String, Object> file : files) {
            String fileName = file.get("FILE_NAME").toString();
            String targetPath = file.get("TARGET_PATH").toString();
            for (Map<String, Object> record : records) {
                if (fileName.startsWith(record.get("FILE_NAME").toString())) {
                    int need_ods = (Integer)record.get("NEED_ODS");
                    if (need_ods == 2) {
                        newProcessGM(targetPath, fileName, backupdir, record,groupidmap,file.get("UPLOAD_GUID").toString());
                    } else {
                        newProcessFile(targetPath, fileName, backupdir, record, groupidmap,
                                file.get("UPLOAD_GUID").toString(), file.get("UPLOADER").toString());
                    }
                    break;
                }
            }
        }
    }

    private void newProcessGM(String fullFileName, String fileName, String backupdir, Map<String, Object> record,
                              Map<String,String> groupidmap, String guid) {
        String[] ss = fileName.split("\\_");
        String group_id = ss[1];
        String now = ss[2];
        if (groupidmap.containsKey(group_id)) {
            group_id = groupidmap.get(group_id);
        }
        try {
            String service = record.get("ODS_SERVICE").toString();
            if (service.equals("JC")) {
                etlGM.initPCBase(fullFileName, now, group_id);
            } else if (service.equals("YE")) {
                etlGM.initBalance(fullFileName, now, group_id);
            } else if (service.equals("MRFSJC")) {
                etlGM.initGMOMRFSJC(fullFileName, now, group_id);
            } else if (service.equals("MRFSFS")) {
                etlGM.initGMOMRFSFS(fullFileName, now, group_id);
            } else if (service.equals("TYCKJC")) {
                etlGM.initGMOTYCKJC(fullFileName, now, group_id);
            } else if (service.equals("TYCKFS")) {
                etlGM.initGMOTYCKFS(fullFileName, now, group_id);
            } else if (service.equals("TYJDJC")) {
                etlGM.initGMOTYJDJC(fullFileName, now, group_id);
            } else if (service.equals("TYJDFS")) {
                etlGM.initGMOTYJDFS(fullFileName, now, group_id);
            }
            String filler1 = String.format("导入完成");
            String usql = "update GP_BM_ID_UPLOADLOG set FILLER1 = '"+filler1+"' where UPLOAD_GUID" +
                    " = '" + guid + "'";
            logger.info(">>><<<" + usql);
            jdbcTemplate.execute(usql);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void newProcessFile(String fullFileName, String fileName, String backupdir, Map<String, Object> record,
                                Map<String,String> groupidmap, String guid, String user) {
        String[] ss = fileName.split("\\_");
        String type = ss[0];
        String group_id = ss[1];
        if (groupidmap.containsKey(group_id)) {
            group_id = groupidmap.get(group_id);
        }
        String now = ss[2];
        try {
            if (true) {
                String groupId = record.get("GROUP_ID").toString();
                if (groupId == null || group_id.equals("")) {
                    groupId = group_id;
                }
                String odsTableName = record.get("TABLE_NAME").toString();
                try {

                    if (record.get("NEED_DELETE").equals("1")) {
                        String sql = String.format("delete from %s where data_date = '%s' and group_id = '%s'", odsTableName
                                , now, groupId);

                        jdbcTemplate.execute(sql);
                    }
                    int sheetNum = Integer.parseInt(record.get("SHEET_NUM").toString());
                    rowcountsuccess = 0;
                    rowcountbase = 0;
                    Date start = new Date();
                    loadExcel(fullFileName, odsTableName, now, sheetNum, groupId);
                    Date end = new Date();
                    String filler1 = String.format("导入完成;总条数:%d;已导入:%d", rowcountbase, rowcountsuccess);
                    String usql = "update GP_BM_ID_UPLOADLOG set FILLER1 = '"+filler1+"' where UPLOAD_GUID" +
                            " = '" + guid + "'";
                    logger.info(">>><<<" + usql);
                    jdbcTemplate.execute(usql);

                    try {
                        usql = String.format("insert into GP_BM_ID_PROCESS_INF(DATA_ID,IMPORT_ID,IMPORT_NAME," +
                                        "IMPORT_STATUS,IMPORT_DESC,IMPORT_OWNER,TOTAL_NUM,PROCESSED_NUM,CORRECT_NUM,ERROR_NUM," +
                                        "FILTER_NUM,START_TIME,END_TIME,DATA_CRT_USER,DATA_CRT_DATE,DATA_CRT_TIME)values('%s','%s'," +
                                        "'%s',9,'导入完成','%s',%d,%d,%d,0,0,'%s','%s','%s','%s','%s')",
                                UuidHelper.randomClean(), UuidHelper.randomClean(),
                                fullFileName.substring(fullFileName.lastIndexOf(File.separator)+1), user, rowcountbase,
                                rowcountsuccess, rowcountsuccess, new SimpleDateFormat("yyyyMMddHHmmss").format(start),
                                new SimpleDateFormat("yyyyMMddHHmmss").format(end), user,
                                new SimpleDateFormat("yyyyMMdd").format(end),
                                new SimpleDateFormat("yyyyMMddHHmmss").format(end));
                        jdbcTemplate.execute(usql);
                    } catch (Exception ex) {
                        log.error("insert process_inf fail", ex);
                    }
                } catch (Exception ex) {
                    log.error("import fail", ex);
                }
                String needOds = record.get("NEED_ODS").toString();
                if (needOds.equals("1")) {
                    try {
                        custLoadFileProcessService.process(odsTableName, now, groupId);
                    } catch (Exception ex) {
                        log.error("process fail", ex);
                    }
                }

                try {
                    //TODO backup file & delete
                    String newbackupdir =
                            backupdir + File.separator + "backup" + File.separator + now + File.separator + fileName;
                    moveFiles(new File(fullFileName).toPath(), new File(newbackupdir).toPath());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (Exception exx) {
            log.error("now error: " + fileName, exx);
        }
    }

    private void processFiles(String dir, String backupdir) {

        File file = new File(dir);
        LinkedList<File> list = new LinkedList<File>();

        if (file.exists()) {
            if (null == file.listFiles()) {
                return;
            }
            List<Map<String, Object>> records = jdbcTemplate.queryForList("select * from ods_ctl order by order_no");
            for (Map<String, Object> record : records) {
                File[] files = file.listFiles();
                for (int i = 0; i < files.length; i++) {
                    if (files[i].getName().endsWith(".xlsx") && !files[i].getName().startsWith(".")) {
                        processFile(files[i].getAbsolutePath(), files[i].getName(), backupdir, record);
                    }
                }
            }

        }
    }

    private void processFile(String fullFileName, String fileName, String backupdir, Map<String, Object> record) {
        if (fileName.length() > 13) {
            String now = fileName.substring(fileName.length()-13, fileName.length()-5);
            String type = "";
            if (fileName.contains("_")) {
                type = fileName.substring(0, fileName.lastIndexOf("_"));
            } else {
                type = fileName.substring(0,fileName.length()-13);
            }
            try {
                if (type.equals(record.get("FILE_NAME").toString())) {
                    String groupId = record.get("GROUP_ID").toString();
                    String odsTableName = record.get("TABLE_NAME").toString();
                    try {

                        if (record.get("NEED_DELETE").equals("1")) {
                            String sql = String.format("delete from %s where data_date = '%s' and group_id = '%s'", odsTableName
                                    , now, groupId);

                            jdbcTemplate.execute(sql);
                        }
                        int sheetNum = Integer.parseInt(record.get("SHEET_NUM").toString());
                        rowcountbase = 0;
                        loadExcel(fullFileName, odsTableName, now, sheetNum, groupId);
                    } catch (Exception ex) {
                        log.error("import fail", ex);
                    }
                    String needOds = record.get("NEED_ODS").toString();
                    if (needOds.equals("1")) {
                        try {
                            custLoadFileProcessService.process(odsTableName, now, groupId);
                        } catch (Exception ex) {
                            log.error("process fail", ex);
                        }
                    }

                    try {
                        //TODO backup file & delete
                        moveFiles(new File(fullFileName).toPath(), new File(backupdir + File.separator + fileName).toPath());
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            } catch (Exception exx) {
                log.error("now error: " + fileName, exx);
            }


        }
    }

    private void moveFiles(Path sourcePath, Path targetPath) throws IOException {
        try {
            Files.move(sourcePath, targetPath, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IOException("移动文件[" + sourcePath + "]到备份文件[" + targetPath + "]失败,请检查文件权限", e);
        }
    }

    private ArrayList<ArrayList<String>> sqls = new ArrayList<ArrayList<String>>();

    private ArrayList<String> puresqls = new ArrayList<String>();

    private int rowcountbase = 0;
    private int rowcountsuccess = 0;

    private int batchCount = 1000;

    private int processcnt = 0;

    private String sql;

    public static boolean isColumnEmpty(Row row, int colno) {
        Cell c = row.getCell(colno);
        if (c == null || c.getCellType() == CellType.BLANK)
            return true;
        return false;
    }

    String getCellValue(Cell cell) {
        if (cell == null)
            return "";
        if (cell.getCellType() == CellType.STRING) {
            return cell.getStringCellValue().trim();
        } else {
            if (cell.getCellType() == CellType.FORMULA)
                return String.valueOf(cell.getNumericCellValue());
            else {
                if (DateUtil.isCellDateFormatted(cell)) {
                    Date date = org.apache.poi.ss.usermodel.DateUtil
                            .getJavaDate(cell.getNumericCellValue());
                    return new SimpleDateFormat("yyyyMMdd").format(date);
                }
                double d = cell.getNumericCellValue();
//                String samount = new DecimalFormat("0.00").format(cell.getNumericCellValue());
//                if (samount.endsWith("0")) {
//                    samount = samount.substring(0, samount.length()-1);
//                }
//                if (samount.endsWith(".0")) {
//                    samount = samount.substring(0, samount.length()-2);
//                }

                return NumberFormat.getInstance().format(cell.getNumericCellValue()).replace(",","");
            }
        }
    }

    public void loadExcel(String filename, String tablename, String data_date, int sheetNum,
                          String group_id) throws Exception {
        FileInputStream in = new FileInputStream(filename);
        try {
            Workbook wk = StreamingReader.builder()
                    .rowCacheSize(100)  //缓存到内存中的行数，默认是10
                    .bufferSize(4096)  //读取资源时，缓存到内存的字节大小，默认是1024
                    .open(in);  //打开资源，必须，可以是InputStream或者是File，注意：只能打开XLSX格式的文件
            Sheet sheet = wk.getSheetAt(sheetNum);
            String columns = "";
            int columncnt = 0;
            sqls.clear();
            //遍历所有的行
            String puresql = "";
            String purevalues = "";
            int expectcolumn = 0;
            for (Row row0 : sheet) {
                if (row0.getRowNum() == 0) {
                    for (Cell cell0 : row0) {
                        expectcolumn++;
                        columns += "`"+cell0.getStringCellValue() + "`,";
                        columncnt++;

                    }
                    columns = columns.substring(0, columns.length() - 1);
                    sql = "insert into " + tablename + "(" + columns + ",data_date, group_id) values (";

                    puresql = sql;
                    for (int k = 0; k < columncnt; k++) {
                        sql += "?,";
                    }
                    sql = sql.substring(0, sql.length() - 1) + ",'"+data_date+"','"+group_id+"')";
                    //System.out.println(sql);
                    break;
                }
            }
            for (Row row : sheet) {
                //System.out.println("开始遍历第" + row.getRowNum() + "行数据：");
                //遍历所有的列
                ArrayList<String> values = null;
                if (row.getRowNum() != 0) {
                    values = new ArrayList<String>();
                    purevalues = "";
                }
                for (int m = 0; m < expectcolumn; m++) {
                    if (row.getRowNum() == -1) {
                        columns += "`"+row.getCell(m).getStringCellValue() + "`,";
                        columncnt++;
                    } else {
                        boolean colFlag = isColumnEmpty(row, m);
                        String valuem = "";
                        if (colFlag)
                            valuem = "";
                        else
                            valuem = getCellValue(row.getCell(m));
                        values.add(valuem);
                        purevalues += "'" + valuem + "',";
                    }
                }
                if (row.getRowNum() == -1) {
                    columns = columns.substring(0, columns.length() - 1);
                    sql = "insert into " + tablename + "(" + columns + ",data_date, group_id) values (";
                    puresql = sql;
                    for (int k = 0; k < columncnt; k++) {
                        sql += "?,";
                    }
                    sql = sql.substring(0, sql.length() - 1) + ",'"+data_date+"','"+group_id+"')";
                    //log.error(sql);
                } else {
                    processcnt++;
                    sqls.add(values);
                    purevalues = purevalues.substring(0, purevalues.length() - 1);
                    puresqls.add(puresql + purevalues + ",'"+data_date+"')");
                    //log.error(puresql + purevalues + ")");
                    if (processcnt == batchCount) {
                        loadBatch();
                    }
                }
            }
            if (processcnt > 0) {
                loadBatch();
            }
        } finally {
            in.close();
        }
    }

    public boolean loadBatch() {
        if (sqls.size() == 0) {
            return true;
        }
        try {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    for (int j = 0; j < sqls.get(i).size(); j++) {
                        ps.setString(j+1, sqls.get(i).get(j));
                    }
                }

                @Override
                public int getBatchSize() {
                    return sqls.size();
                }
            });
            rowcountsuccess += sqls.size();
        } catch (Exception ex) {
            for (int i = 0; i < sqls.size(); i++) {
                try {
                    final ArrayList<String> record = sqls.get(i);
//                    transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
                        jdbcTemplate.update(sql, new PreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement ps) throws SQLException {
                                for (int j = 0; j < record.size(); j++) {
                                    ps.setString(j + 1, record.get(j));
                                }
                            }
                        });
//                    });
                    rowcountsuccess++;
                } catch (Exception singleex) {
                    singleex.printStackTrace();
                }
            }
        } finally {
            rowcountbase += sqls.size();
            sqls.clear();
            puresqls.clear();
            processcnt = 0;
        }
        return true;
    }
}
