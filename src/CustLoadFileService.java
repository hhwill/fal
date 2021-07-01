package com.gingkoo.imas.hsbc.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import javax.sql.DataSource;

import com.monitorjbl.xlsx.StreamingReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import com.gingkoo.gf4j2.framework.service.SysParamService;
import com.gingkoo.imas.core.batch.ImasBatchBasicValidateService;
import com.gingkoo.root.facility.spring.tx.TransactionHelper;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
@Component
public class CustLoadFileService {

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    private ImasBatchBasicValidateService imasBatchBasicValidateService;

    private CustLoadFileProcessService custLoadFileProcessService;

    public CustLoadFileService(TransactionHelper transactionTemplate, DataSource dataSource,
                               ImasBatchBasicValidateService imasBatchBasicValidateService, CustLoadFileProcessService custLoadFileProcessService) {
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.imasBatchBasicValidateService = imasBatchBasicValidateService;
        this.custLoadFileProcessService = custLoadFileProcessService;
    }


    public void run() {
        log.info("CustLoadFileService");
        String importDir = SysParamService.getSysParamDef("IMAS","MANUAL_IMPORT_DIR","");
        String backupDir = SysParamService.getSysParamDef("IMAS","MANUAL_BACKUP_DIR","");
        System.out.println(importDir);
        System.out.println(backupDir);
        processFiles(importDir, backupDir);
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
                    try {
                        String odsTableName = record.get("TABLE_NAME").toString();
                        String sql = String.format("delete from %s where data_date = '%s' and group_id = '%s'", odsTableName
                                , now, groupId);

                        jdbcTemplate.execute(sql);
                        loadExcel(fullFileName, odsTableName, now, groupId);
                    } catch (Exception ex) {
                        log.error("import fail", ex);
                    }
                    String needOds = record.get("NEED_ODS").toString();
                    if (needOds.equals("1")) {
                        try {
                            custLoadFileProcessService.process(type, now, groupId);
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
                return "="+cell.getCellFormula();
            else {
                if (DateUtil.isCellDateFormatted(cell)) {
                    Date date = org.apache.poi.ss.usermodel.DateUtil
                            .getJavaDate(cell.getNumericCellValue());
                    return new SimpleDateFormat("yyyyMMdd").format(date);
                }
                String samount = new DecimalFormat("0.00").format(cell.getNumericCellValue());
                if (samount.endsWith("0")) {
                    samount = samount.substring(0, samount.length()-1);
                }
                if (samount.endsWith(".0")) {
                    samount = samount.substring(0, samount.length()-2);
                }
                return samount;
            }
        }
    }

    public void loadExcel(String filename, String tablename, String data_date, String group_id) throws Exception {
        FileInputStream in = new FileInputStream(filename);
        try {
            Workbook wk = StreamingReader.builder()
                    .rowCacheSize(100)  //缓存到内存中的行数，默认是10
                    .bufferSize(4096)  //读取资源时，缓存到内存的字节大小，默认是1024
                    .open(in);  //打开资源，必须，可以是InputStream或者是File，注意：只能打开XLSX格式的文件
            Sheet sheet = wk.getSheetAt(0);
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
        } catch (Exception ex) {
            for (int i = 0; i < sqls.size(); i++) {
                try {
                    final ArrayList<String> record = sqls.get(i);
                    transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
                        jdbcTemplate.update(sql, new PreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement ps) throws SQLException {
                                for (int j = 0; j < record.size(); j++) {
                                    ps.setString(j + 1, record.get(j));
                                }
                            }
                        });
                    });
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
