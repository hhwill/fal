import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import oracle.jdbc.driver.OracleDriver;

import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainApplication {

    private static final Logger log = LoggerFactory.getLogger(MainApplication.class);

    public static int batchCount = 3;

    public static String SQL_PJTXFS = "INSERT INTO `IMAS_PM_PJTXFS`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`YWBM`,\n" +
            "`JYLSH`,\n" +
            "`BZ`,\n" +
            "`JYRQ`,\n" +
            "`FSJE`,\n" +
            "`TXLL`,\n" +
            "`JYFX`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_PJTXJC = "INSERT INTO `IMAS_PM_PJTXJC`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JRJGLXDM`,\n" +
            "`YWBM`,\n" +
            "`PJRZYWLX`,\n" +
            "`QSRQ`,\n" +
            "`DQRQ`,\n" +
            "`PJRZQXLX`,\n" +
            "`TXLL`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_PJTXYE = "INSERT INTO `imas`.`IMAS_PM_PJTXYE`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`YWBM`,\n" +
            "`BZ`,\n" +
            "`YE`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_TYJDFS = "INSERT INTO `IMAS_PM_TYJDFS`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`YWBM`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JYLSH`,\n" +
            "`JYRQ`,\n" +
            "`BZ`,\n" +
            "`SJLL`,\n" +
            "`JZLL`,\n" +
            "`FSJE`,\n" +
            "`JYFX`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?," +
            "?," +
            "?)";
    public static String SQL_TYJDJC = "INSERT INTO `IMAS_PM_TYJDJC`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JRJGLXDM`,\n" +
            "`YWBM`,\n" +
            "`JDYWLX`,\n" +
            "`QSRQ`,\n" +
            "`DQRQ`,\n" +
            "`SJZZRQ`,\n" +
            "`TYJDQXLX`,\n" +
            "`LLLX`,\n" +
            "`SJLL`,\n" +
            "`JDDJJZLX`,\n" +
            "`JZLL`,\n" +
            "`JXFS`,\n" +
            "`LLFDPL`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?," +
            "?)";
    public static String SQL_TYJDYE = "INSERT INTO `IMAS_PM_TYJDYE`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`YWBM`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`BZ`,\n" +
            "`YE`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_DWDKFK = "INSERT INTO `IMAS_PM_DWDKFK`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`DKJJBH`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`JYLSH`,\n" +
            "`JYRQ`,\n" +
            "`BZ`,\n" +
            "`FSJE`,\n" +
            "`JZLL`,\n" +
            "`SJLL`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_DWDKJC = "INSERT INTO `IMAS_PM_DWDKJC`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`DKHTBM`,\n" +
            "`DKJJBH`,\n" +
            "`DKCPLB`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`DKFFRQ`,\n" +
            "`YSDQRQ`,\n" +
            "`SJZZRQ`,\n" +
            "`DKQXLX`,\n" +
            "`LLLX`,\n" +
            "`DJJZLX`,\n" +
            "`JZLL`,\n" +
            "`SJLL`,\n" +
            "`LLFDPL`,\n" +
            "`DKSJTX`,\n" +
            "`DKBLQD`,\n" +
            "`CZBL`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0'," +
            "?,?,?)";
    public static String SQL_DWDKYE = "INSERT INTO `IMAS_PM_DWDKYE`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`DKJJBH`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`BZ`,\n" +
            "`DKYE`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_GRKHXX = "INSERT INTO `IMAS_PM_GRKHXX`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`CZDHZQHDM`,\n" +
            "`SXED`,\n" +
            "`YYED`,\n" +
            "`KHXL`,\n" +
            "`NHBZ`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";
    public static String SQL_DGKHXX = "INSERT INTO `imas`.`IMAS_PM_DGKHXX`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`KHH`,\n" +
            "`NBJGH`,\n" +
            "`GMJJBMFL`,\n" +
            "`JRJGLXDM`,\n" +
            "`QYGM`,\n" +
            "`KGLX`,\n" +
            "`JNJWBZ`,\n" +
            "`JYSZDHZQHDM`,\n" +
            "`ZCDZ`,\n" +
            "`SXED`,\n" +
            "`YYED`,\n" +
            "`SSHY`,\n" +
            "`NCCSBZ`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    public static String SQL_DWCKJC = "INSERT INTO `IMAS_PM_DWCKJC`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`CKZHBM`,\n" +
            "`CKXH`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`CKCPLB`,\n" +
            "`XYCKLX`,\n" +
            "`QSRQ`,\n" +
            "`DQRQ`,\n" +
            "`SJZZRQ`,\n" +
            "`CKQXLX`,\n" +
            "`DJJZLX`,\n" +
            "`LLLX`,\n" +
            "`SJLL`,\n" +
            "`JZLL`,\n" +
            "`LLFDPL`,\n" +
            "`BDSYL`,\n" +
            "`ZGSYL`,\n" +
            "`KHQD`,\n" +
            "`YDCKBZ`,\n" +
            "`DXEBZ`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00'," +
            "'A','2','0',?,?,?)";

    public static String SQL_DWCKYE = "INSERT INTO `IMAS_PM_DWCKYE`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`CKZHBM`,\n" +
            "`CKXH`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`BZ`,\n" +
            "`CKYE`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    public static String SQL_TYCKJC = "INSERT INTO `IMAS_PM_TYCKJC`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JRJGLXDM`,\n" +
            "`CKZHBM`,\n" +
            "`CFYWLX`,\n" +
            "`QSRQ`,\n" +
            "`DQRQ`,\n" +
            "`CKQXLX`,\n" +
            "`DJJZLX`,\n" +
            "`LLLX`,\n" +
            "`SJLL`,\n" +
            "`JZLL`,\n" +
            "`LLFDPL`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    public static String SQL_TYCKYE = "INSERT INTO `IMAS_PM_TYCKYE`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`CKZHBM`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`BZ`,\n" +
            "`YE`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    public static String SQL_TYCKFS = "INSERT INTO `IMAS_PM_TYCKFS`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`CKZHBM`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JYLSH`,\n" +
            "`JYRQ`,\n" +
            "`BZ`,\n" +
            "`SJLL`,\n" +
            "`JZLL`,\n" +
            "`FSJE`,\n" +
            "`JYFX`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    public static String SQL_DWCKFS = "INSERT INTO `IMAS_PM_DWCKFS`\n" +
            "(`DATA_ID`,\n" +
            "`DATA_RPT_DATE`,\n" +
            "`ORG_ID`,\n" +
            "`GROUP_ID`,\n" +
            "`SJRQ`,\n" +
            "`CKZHBM`,\n" +
            "`CKXH`,\n" +
            "`NBJGH`,\n" +
            "`KHH`,\n" +
            "`JYLSH`,\n" +
            "`JYRQ`,\n" +
            "`SJLL`,\n" +
            "`JZLL`,\n" +
            "`BZ`,\n" +
            "`FSJE`,\n" +
            "`JYQD`,\n" +
            "`JYFX`,\n" +
            "`DXEBZ`,\n" +
            "`CHECK_FLAG`,\n" +
            "`NEXT_ACTION`,\n" +
            "`DATA_RPT_FLAG`,\n" +
            "`DATA_STATUS`,\n" +
            "`DATA_FLAG`,\n" +
            "`DATA_SOURCE`,\n" +
            "`DATA_VERSION`,\n" +
            "`DATA_CRT_USER`,\n" +
            "`DATA_CRT_DATE`,\n" +
            "`DATA_CRT_TIME`) VALUES (?,?,'HSBC',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'N','00','A','00','A','2','0',?,?,?)";

    private boolean insertData(String sql, String groupId, String user, List<List<String>> params) {
        int times = params.size() / 1000;
        if (params.size() % 1000 != 0) {
            times += 1;
        }
        for (int i = 0; i < times; i++) {
            List<List<String>> currentUpdates = new ArrayList<List<String>>();
            if (i == times - 1) {
                for (int m = i * 1000; m < params.size(); m++) {
                    currentUpdates.add(params.get(m));
                }
            } else {
                for (int m = i * 1000; m < (i + 1) * 1000; m++) {
                    currentUpdates.add(params.get(m));
                }
            }
            batchInsert(sql, groupId, user, currentUpdates);
        }
        return true;
    }

    private boolean batchInsert(String sql, String groupId, String user, List<List<String>> params) {
        Date now = new Date();
        String date = new SimpleDateFormat("yyyyMMdd").format(now);
        String time = new SimpleDateFormat("yyyyMMddhhmmss").format(now);
        try {
            if (conn == null) {
                Driver driver = new com.mysql.cj.jdbc.Driver();
                DriverManager.deregisterDriver(driver);
                Properties pro = new Properties();
                pro.put("user", properties.getProperty("jdbc.username"));
                pro.put("password", properties.getProperty("jdbc.password"));
                conn = driver.connect(properties.getProperty("jdbc.url"), pro);
            }
        } catch (Exception ex) {
            System.out.println("无法连接数据库");
            return false;
        }
        try {
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.size(); i++) {
                int index = 1;
                List<String> param = params.get(i);
                pstmt.setString(index, UUID.randomUUID().toString().replace("-", ""));
                index++;
                pstmt.setString(index, param.get(0));
                index++;
                pstmt.setString(index, groupId);
                index++;
                for (int j = 0; j < param.size(); j++) {
                    if (param.get(j) == null || param.get(j).equals("")) {
                        pstmt.setNull(index, Types.INTEGER);
                    } else {
                        pstmt.setString(index, param.get(j));
                    }
                    index++;
                }
                pstmt.setString(index, user);
                index++;
                pstmt.setString(index, date);
                index++;
                pstmt.setString(index, time);
                //log.error(sqls.get(i).toString());
                pstmt.addBatch();
            }
            try {
                pstmt.executeBatch();
            } catch (BatchUpdateException exx) {
                //log.error("batch失败"+String.valueOf(rowcountbase),exx);
                //单独执行
                //statement = conn.createStatement();
                for (int i = 0; i < params.size(); i++) {
                    int index = 1;
                    List<String> param = params.get(i);
                    pstmt.setString(index, UUID.randomUUID().toString().replace("-", ""));
                    index++;
                    pstmt.setString(index, param.get(0));
                    index++;
                    pstmt.setString(index, groupId);
                    index++;
                    for (int j = 0; j < param.size(); j++) {
                        if (param.get(j) == null || param.get(j).equals("")) {
                            pstmt.setNull(index, Types.INTEGER);
                        } else {
                            pstmt.setString(index, param.get(j));
                        }
                        index++;
                    }
                    pstmt.setString(index, user);
                    index++;
                    pstmt.setString(index, date);
                    index++;
                    pstmt.setString(index, time);
                    try {
                        pstmt.execute();
                    } catch (Exception exxx) {
                        exxx.printStackTrace();
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("执行批量插入准备失败");
            ex.printStackTrace();
        } finally {
        }
        return true;
    }

    private String sql;

    public Properties properties = new Properties();

    private ArrayList<ArrayList<String>> sqls = new ArrayList<ArrayList<String>>();

    private ArrayList<String> puresqls = new ArrayList<String>();

    private int rowcountbase = 0;

    private int processcnt = 0;

    Connection conn = null;
    Statement statement = null;
    ResultSet resultSet = null;
    PreparedStatement pstmt = null;

    public void loadProperties() {
        String curDir = System.getProperty("user.dir");
        File file=new File(curDir + File.separator + "application.properties");

        try
        {
            FileInputStream fileInputStream=new FileInputStream(file);
            properties.load(fileInputStream);
            batchCount = Integer.parseInt(properties.getProperty("batch.count"));
            //System.out.print("xxx"+String.valueOf(batchCount));
        }
        catch (Exception e)
        {
            System.out.println("当前工作目录["+curDir+"]下不存在配置文件application.properties或格式不正确");
        }
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

    private String parseTableName(String filename) {
        Set<Object> keys = properties.keySet();
        for (Object key : keys) {
            if (filename.contains(key.toString()))
                return properties.getProperty(key.toString());
        }
        return "";
    }

    public boolean loadBatch() {
        if (sqls.size() == 0) {
            return true;
        }
        try {
            if (conn == null) {
                Driver driver = new com.mysql.cj.jdbc.Driver();
                DriverManager.deregisterDriver(driver);
                Properties pro = new Properties();
                pro.put("user", properties.getProperty("jdbc.username"));
                pro.put("password", properties.getProperty("jdbc.password"));
                conn = driver.connect(properties.getProperty("jdbc.url"), pro);
            }
        } catch (Exception ex) {
            System.out.println("无法连接数据库");
            return false;
        }
        try {
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < sqls.size(); i++) {
                for (int j = 0; j < sqls.get(i).size(); j++) {
                    pstmt.setString(j+1, sqls.get(i).get(j));
                }
                //log.error(sqls.get(i).toString());
                pstmt.addBatch();
            }
            try {
                pstmt.executeBatch();
            } catch (BatchUpdateException exx) {
                //log.error("batch失败"+String.valueOf(rowcountbase),exx);
                //单独执行
                //statement = conn.createStatement();
                for (int i = 0; i < sqls.size(); i++) {
                    for (int j = 0; j < sqls.get(i).size(); j++) {
                        pstmt.setString(j+1, sqls.get(i).get(j));
                    }
                    try {
                        pstmt.execute();
                    } catch (Exception exxx) {
                        log.error("第"+String.valueOf(rowcountbase+i+2) + "行插入出错,sql["+puresqls.get(i)+"]错误信息:"+exxx.getMessage());
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("执行批量插入准备失败");
            ex.printStackTrace();
        } finally {
            rowcountbase += sqls.size();
            sqls.clear();
            puresqls.clear();
            processcnt = 0;
        }
        return true;
    }

    public void clearConnect() {
        try {
            if (resultSet!=null) resultSet.close();
            if (statement!=null) statement.close();
            if (pstmt!=null) pstmt.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static boolean isColumnEmpty(Row row, int colno) {
        Cell c = row.getCell(colno);
        if (c == null || c.getCellType() == CellType.BLANK)
            return true;
        return false;
    }

    public void loadExcel(String filename, String tablename, String data_date, int all) throws Exception {
        FileInputStream in = new FileInputStream(filename);
        try {
            Workbook wk = StreamingReader.builder()
                    .rowCacheSize(100)  //缓存到内存中的行数，默认是10
                    .bufferSize(4096)  //读取资源时，缓存到内存的字节大小，默认是1024
                    .open(in);  //打开资源，必须，可以是InputStream或者是File，注意：只能打开XLSX格式的文件
            for (int i = 0; i < all; i++) {
                Sheet sheet = wk.getSheetAt(i);
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
                            columns += "`" + cell0.getStringCellValue() + "`,";
                            columncnt++;

                        }
                        columns = columns.substring(0, columns.length() - 1);
                        sql = "insert into " + tablename + "(" + columns + ",data_date) values (";

                        puresql = sql;
                        for (int k = 0; k < columncnt; k++) {
                            sql += "?,";
                        }
                        sql = sql.substring(0, sql.length() - 1) + ",'" + data_date + "')";
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
                            columns += "`" + row.getCell(m).getStringCellValue() + "`,";
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
                        sql = "insert into " + tablename + "(" + columns + ",data_date) values (";
                        puresql = sql;
                        for (int k = 0; k < columncnt; k++) {
                            sql += "?,";
                        }
                        sql = sql.substring(0, sql.length() - 1) + ")";
                        //log.error(sql);
                    } else {
                        processcnt++;
                        sqls.add(values);
                        purevalues = purevalues.substring(0, purevalues.length() - 1);
                        puresqls.add(puresql + purevalues + ",'" + data_date + "')");
                        //log.error(puresql + purevalues + ")");
                        if (processcnt == batchCount) {
                            loadBatch();
                        }
                    }
                }
                if (processcnt > 0) {
                    loadBatch();
                }
            }
        } finally {
            in.close();
        }
    }

    public void printCreateSql(String filename, String tablename, String key) throws Exception {
        FileInputStream in = new FileInputStream(filename);
        try {
            Workbook wk = StreamingReader.builder()
                    .rowCacheSize(100)  //缓存到内存中的行数，默认是10
                    .bufferSize(4096)  //读取资源时，缓存到内存的字节大小，默认是1024
                    .open(in);  //打开资源，必须，可以是InputStream或者是File，注意：只能打开XLSX格式的文件
            Sheet sheet = wk.getSheetAt(0);
            for (Row row : sheet) {
                if (row.getRowNum() == 0) {
                    String s = "create table `"+tablename+"` (\n";
                    for (Cell cell : row) {
                        s += "`" + getCellValue(cell) + "` varchar(100) DEFAULT NULL,\n";
                    }
                    s+= "`DATA_DATE` VARCHAR(8),GROUP_ID VARCHAR(100),\nPRIMARY KEY (`DATA_DATE`,`GROUP_ID`,`"+key+
                            "`) )";
                    System.out.println(s);
                    break;
                }
            }

        } finally {
            in.close();
        }
    }

    public static int differentDaysByMillisecond(String date1, String date2)
    {
        try {
            Date ddate1 = new SimpleDateFormat("yyyyMMdd").parse(date1);
            Date ddate2 = new SimpleDateFormat("yyyyMMdd").parse(date2);
            int days = (int) ((ddate2.getTime() - ddate1.getTime()) / (1000 * 3600 * 24));
            return Math.abs(days);
        } catch (Exception ex) {
            return 0;
        }
    }

    private String getMap(String type_no, String key) {
        String result = "";
        result = map.get(type_no).get(key);
        if (result == null || result.equals("null")) {
            result = "";
        }
        return result;
    }
    private String checkTyjdTenor(String sdays, Map<String, String> dict) {
        if (sdays.equals("")) {
            if (dict.containsKey("")) {
                return dict.get("");
            }
        }
        int days = Integer.parseInt(sdays);
        String result = "";
        for (String key : dict.keySet()) {
            if (key.contains("-")) {
                String[] ss = key.split("-");
                int s0 = Integer.parseInt(ss[0]);
                int s1 = Integer.parseInt(ss[1]);
                if (days >= s0 && days <= s1) {
                    return dict.get(key);
                }
            } else if (key.startsWith(">")) {
                int s0 = Integer.parseInt(key.substring(1));
                if (days >= s0) {
                    return dict.get(key);
                }
            } else {
                int s0 = Integer.parseInt(key);
                if (days == s0) {
                    return dict.get(key);
                }
            }
        }
        return result;
    }

    private List<String> addFtyscsaiBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("C3BLRF"));
        result.add(src.get("C3BLRF"));
        result.add("F082");
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));
        result.add(src.get("C3RCDT"));
        result.add(src.get("C3DUDT"));
        if (src.get("C3INVA").equals("0")) {
            result.add(now);
        } else {
            result.add("");
        }

        int days = differentDaysByMillisecond(src.get("C3DUDT"), src.get("C3ISDT"));
        result.add(checkTyjdTenor(String.valueOf(days), map.get("X0")));
        result.add("RF01");
        String c3inty = src.get("C3INTY");
        String c3cycd = src.get("C3CYCD");
        String c3cuno = src.get("C3CUNO");
        String effectiveDate = getMap("X34", c3cuno);
        days = differentDaysByMillisecond(src.get("C3RCDT"), effectiveDate);
        if (days > 0 && c3cycd.equals("CNY")) {
            c3inty = "LP1";
        }
        result.add(getMap("X33", c3inty));
        result.add("0");
        result.add(src.get("C3INMG"));
        result.add("0");
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

    private List<String> addFtyscsaiBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("C3BLRF"));
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));
        result.add(src.get("C3CYCD"));
        result.add(formatJPY(src.get("C3CYCD"),src.get("C3INVA")));
        return result;
    }

    private List<String> addFtyscsaiOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("C3BLRF"));
        result.add(formatKHH(src.get("C3CUNO")));
        result.add(formatNBJGH(getMap("X31", src.get("C3CUNO"))));


        result.add(src.get("C3BLRF")+src.get("交易方向"));
        result.add(src.get("C3RCDT"));
        result.add(src.get("C3CYCD"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("C3BLAM")));
        result.add("0");
        result.add(src.get("C3INMG"));
        return result;
    }

    private List<String> addFtydwdkBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(src.get("BILLREF"));
        String zzlf04 = src.get("ZZLF04");
        if (zzlf04 != null) {
            if (zzlf04.equals("1")) {
                result.add("F082");
            } else if (zzlf04.equals("0")) {
                result.add("F081");
            } else {
                result.add("");
            }
        } else {
            result.add("");
        }
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(src.get("BBDTAV"));
        result.add(src.get("BBDUDT"));
        //TODO 结清填当天
        if (src.get("ADVOS").equals("0"))
            result.add(now);
        else
            result.add("");
        String days = src.get("BBUSAN");
        result.add(checkTyjdTenor(days, map.get("X0")));
        result.add("RF01");
        result.add(getMap("X22", src.get("BBDRTY")));
        result.add("");
        result.add(src.get("BBDRSP"));
        result.add("");
        String zzlf05 = src.get("ZZLF05");
        if (zzlf05 != null && zzlf05.length() >= 3) {
            result.add(getMap("X24",  zzlf05.substring(0,3)));
        } else{
            result.add("");
        }
        result.add("01");
        result.add("");
        return result;
    }

    private List<String> addFtydwdkBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addFtydwdkOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatKHH(src.get("CUS")));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));


        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(now);
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("BILLAMT")));
        result.add("");
        String bbdrsp = src.get("BBDRSP");
        if (src.get("BBDRTY").equals("LP1")) {
           bbdrsp =  new BigDecimal(bbdrsp).add(new BigDecimal("3.85")).toString();
        }
        result.add(bbdrsp);
        return result;
    }

    private List<String> addTyjdBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(getMap("X13", src.get("BAACSN")));
        result.add(src.get("BILLREF"));
        result.add("A01");
        result.add(src.get("BBDTAV"));
        result.add(src.get("BBDUDT"));
        //TODO 结清填当天
        result.add("");
        String days = src.get("BBUSAN");
        result.add(checkTyjdTenor(days, map.get("X14")));
        result.add("RF01");
        result.add(src.get("BBDRSP"));
        result.add(getMap("X12", src.get("BBDRTY")));
        result.add("");
        result.add("04");
        result.add("");
        return result;
    }

    private List<String> addTyjdBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addTyjdOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(formatNBJGH(src.get("ACCOUNTNO").substring(0,3)));
        result.add(formatKHH(src.get("CUS")));

        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(now);
        result.add(src.get("BBPRCY"));
        result.add(src.get("BBDRSP"));
        result.add(src.get("BILLAMT"));
        result.add(src.get("交易方向"));
        return result;
    }

    private String formatNBJGH(String src) {
        String result = src;
        while (result.length() < 3) {
            result = "0" + result;
        }
        if (result.length() == 3) {
            result = "CNHSBC"+ result;
        }
        return result;
    }

    private String formatKHH(String src) {
        String result = src;
        if (result.startsWith("CNHSBC")) {
            return result;
        }
        if (result.contains("-")) {
            String[] ss = result.split("-");

            while (ss[0].length() < 3) {
                ss[0] = "0" + ss[0];
            }
            if (ss[0].length() > 3) {
                ss[0] = ss[0].substring(0,3);
            }
            while (ss[1].length() < 6) {
                ss[1] = "0" + ss[1];
            }
            if (ss[1].length() > 6) {
                ss[1] = ss[1].substring(0,6);
            }
            result = ss[0] + ss[1];
        }
        result = "CNHSBC" + result;
        if (result.length() >15) {
            result = result.substring(0,15);
        }
        return result;
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

    public String formatJPY(String ccy, String src) {
        String result = src;
        if (ccy == null) {
            return src;
        }
        if (!ccy.equals("JPY") && !ccy.equals("KRW") ) {
            return result;
        }
        if (!src.contains(".")) {
            result = src + "00";
        } else {
            if (src.indexOf(".") == src.length()-1) {
                result = src.substring(0, src.indexOf(".")) + "00";
            } else if (src.indexOf(".") == src.length()-2) {
                result = src.substring(0, src.indexOf(".")) + src.substring(src.length()-1) + "0";
            } else if (src.indexOf(".") == src.length()-3) {
                result = src.substring(0, src.indexOf(".")) + src.substring(src.length()-2);
            }
        }
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

    public void processGRKHXX(String now, List<Map<String, String>> lstNow,
                                  List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            base.add(addGRKHXX(now, record));
        }
        insertData(SQL_GRKHXX, "OPS_BOS", "GTRF_GRKHXX", base);
    }

    public boolean checkDGKHXX(List<List<String>> base, Map<String, String> src) {
        boolean find = false;
        String khh = formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS"));
        for (int i = 0; i < base.size(); i++) {
            if (base.get(i).get(1).equals(khh)) {
                find = true;
                String ZUCSSN = src.get("ZUCSSN");
                if (ZUCSSN.contains("PARENT") || ZUCSSN.startsWith("P")) {
                } else {
                    String ZUIDTY = src.get("ZUIDTY");
                    if (ZUIDTY.equals("Z")) {
                        String ZUIDNO = src.get("ZUIDNO");
                        if (ZUIDNO.trim().length() == 18) {
                            String dqdm = ZUIDNO.substring(2,8);
                            if (getMap("DQQHDM", dqdm).equals("")) {
                                dqdm = dqdm.substring(0,4) + "00";
                                if (getMap("DQQHDM", dqdm).equals("")) {
                                    dqdm = dqdm.substring(0,2) + "0000";
                                    if (getMap("DQQHDM", dqdm).equals("")) {

                                    } else {
                                        base.get(i).set(8, dqdm);
                                    }
                                } else {
                                    base.get(i).set(8, dqdm);
                                }
                            } else {
                                base.get(i).set(8, dqdm);
                            }
                        }
                    }
                }
                String ZBADID = src.get("ZBADID");
                if (ZBADID.equals("09")) {
                    base.get(i).set(9, src.get("ADDRESS").replace("（注册地址）","").trim());
                }
                break;
            }
        }
        return find;
    }

    private List<String> addWCAS_DGKHXX(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS")));
        String nbjgh = formatNBJGH(src.get("ZGDCB"));
        result.add(nbjgh);
        String ZGCUCL = src.get("ZGCUCL");
        String ZGC2CN = src.get("ZGC2CN");
        String gmjjbmfl = "";
        if ("/COE/POE/SOE/UCG/UCN/".contains("/"+ZGCUCL+"/")) {
            if (ZGC2CN.contains("公司")) {
                gmjjbmfl = "C01";
            } else {
                gmjjbmfl = "C02";
            }
        } else {
            gmjjbmfl = getMap("X41", ZGCUCL).trim();
        }
        result.add(gmjjbmfl);

        String JRJGLXDM = getMap("X42", ZGCUCL).trim();
        if (JRJGLXDM.length() > 3) {
            JRJGLXDM = JRJGLXDM.substring(0,3);
        }
        if (JRJGLXDM.equals("0")) {
            JRJGLXDM = "";
        }
        result.add(JRJGLXDM);
        String qygm = "";
        if ("/CCG/GVP/GVT/GAO/SOF/AMY/HPF/CBK/SAF/".contains("/"+ZGCUCL+"/")) {
            qygm = "CS05";
        } else {
            String size = getMap("X44", formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS")).substring(6));
            if (size.equals("L")) {
                qygm = "CS01";
            } else if (size.equals("M")) {
                qygm = "CS02";
            } else if (size.equals("S")) {
                qygm = "CS03";
            } else if (size.equals("W")) {
                qygm = "CS04";
            } else {
                String XUSLTO = src.get("XUSLTO");
                if (XUSLTO.equals("5")) {
                    qygm = "CS01";
                } else if (XUSLTO.equals("2") || XUSLTO.equals("4") || XUSLTO.equals("3")) {
                    qygm = "CS02";
                } else {
                    String XUEMPE = src.get("XUEMPE");
                    if (XUEMPE.equals("L") || (XUEMPE.equals("O"))) {
                        qygm = "CS04";
                    } else if (XUEMPE.trim().length()> 1 && !XUEMPE.equals("0")) {
                        qygm = "CS03";
                    } else {
                        String LMSI = src.get("S@LMSI");
                        if (LMSI.equals("L")) {
                            qygm = "CS01";
                        } else if (LMSI.equals("M")) {
                            qygm = "CS02";
                        }
                    }
                }
            }
        }
        result.add(qygm);
        String kglx = "";
        if ("/CCG/GVP/GVT/GAO/SOF/AMY/HPF/CBK/SAF/".contains("/"+ZGCUCL+"/")) {

        } else {
            String finalShareHolder = getMap("X45", formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS")).substring(6));
            if (finalShareHolder.equals("STATE")) {
                kglx = "A01";
            } else if (finalShareHolder.equals("PRIVATE")) {
                kglx = "B01";
            } else if (finalShareHolder.equals("HKMATW")) {
                kglx = "B02";
            } else if (finalShareHolder.equals("FOREIGN")) {
                kglx = "B03";
            } else {
                String cbkglx = getMap("X43", ZGCUCL);
                if (cbkglx.length() == 3) {
                    kglx = cbkglx;
                } else {
                    String ZGGHCL = src.get("ZGGHCL");
                    String XUCTHQ = src.get("XUCTHQ");
                    if (cbkglx.equals("A01/B01/B02/B03")) {
                        if ("/RCA/RCB/RCC/".contains("/"+ZGGHCL+"/")) {
                            if (XUCTHQ.equals("CN")) {
                                kglx = "B01";
                            } else if (XUCTHQ.equals("HK")||XUCTHQ.equals("AM")||XUCTHQ.equals("TW")) {
                                kglx = "B02";
                            } else {
                                kglx = "B03";
                            }
                        } else {
                            if (XUCTHQ.equals("CN")) {
                                kglx = "A01";
                            } else if (XUCTHQ.equals("HK")||XUCTHQ.equals("AM")||XUCTHQ.equals("TW")) {
                                kglx = "B02";
                            } else {
                                kglx = "B03";
                            }
                        }
                    } else if (cbkglx.equals("B02/B03")) {
                        if (XUCTHQ.equals("HK")||XUCTHQ.equals("AM")||XUCTHQ.equals("TW")) {
                            kglx = "B02";
                        } else {
                            kglx = "B03";
                        }
                    }
                }
            }
        }
        result.add(kglx);
        result.add("Y");
        result.add(getMap("XDQDM", nbjgh));
        result.add(src.get("ADDRESS").replace("（注册地址）","").trim());
        result.add("0");
        result.add("0");
        result.add(getMap("X46", src.get("ZGINDY")));
        result.add("");
        return result;
    }

    public void processWCAS_DGHKXX(String now, List<Map<String, String>> lstNow,
                              List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            if (!checkDGKHXX(base, record)) {
                base.add(addWCAS_DGKHXX(now, record));
            }
        }
        insertData(SQL_DGKHXX, "OPS_WCAS", "WCAS_DGKHXX", base);
    }

    public void processGRKHXXBASE(String now, List<Map<String, String>> lstNow,
                                  List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            base.add(addGRKHXXBASE(now, record));
        }
        insertData(SQL_GRKHXX, "OPS_BOS", "GTRF_GRKHXX_BASE", base);
    }

    public void processFTYSCSAI(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("C3BLRF");
            String c3isdt = record.get("C3ISDT");
            if (c3isdt == null || c3isdt.equals("0")) {
                continue;
            }
            boolean find = false;
            boolean matchOccur = false;
            boolean matchBase = false;
            String advos = record.get("C3INVA");
            if (advos != null && !advos.equals("0")) {
                matchBase = true;
            }
            for (Map<String, String> orecord : lstPrevious) {
                String oc3isdt = orecord.get("C3ISDT");
                if (oc3isdt == null || oc3isdt.equals("0")) {
                    continue;
                }
                String obillref = orecord.get("C3BLRF");
                if (obillref.equals(billref)) {
                    find = true;
                    String badvos = orecord.get("C3INVA");
                    if (badvos == null || badvos.equals("0")) {
                        break;
                    }
                    if (new BigDecimal(advos).compareTo(new BigDecimal(badvos)) == 1) {
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
                base.add(addFtyscsaiBase(now, record));
                balance.add(addFtyscsaiBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addFtyscsaiOccur(now, record));
            }
        }
        insertData(SQL_DWDKFK, "GTRF_RFN", "GTRF_FTYSCSAI", occur);
        insertData(SQL_DWDKYE, "GTRF_RFN", "GTRF_FTYSCSAI", balance);
        insertData(SQL_DWDKJC, "GTRF_RFN", "GTRF_FTYSCSAI", base);
    }

    public void processCORPDDAC(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String DFSTUS = record.get("DFSTUS");
            if (DFSTUS == null || DFSTUS.equals("4") || DFSTUS.equals("5") ) {
                dwckjc.add(addWCASDWCKJC_CORPDDAC(now, record));
                dwckye.add(addWCASDWCKYE_CORPDDAC(now, record));
            } else {
                tyckjc.add(addWCASTYCKJC_CORPDDAC(now, record));
                tyckye.add(addWCASTYCKYE_CORPDDAC(now, record));
            }
        }
        insertData(SQL_DWCKJC, "OPS_WCAS","OPS_WCAS", dwckjc);
        insertData(SQL_DWCKYE, "OPS_WCAS", "OPS_WCAS", dwckye);
        insertData(SQL_TYCKJC, "OPS_WCAS", "OPS_WCAS", tyckjc);
        insertData(SQL_TYCKYE, "OPS_WCAS", "OPS_WCAS", tyckye);
//        insertData(SQL_DWDKFK, "GTRF_Core_Trade", "GTRF_FTYDWDK", occur);
//        insertData(SQL_DWDKYE, "GTRF_Core_Trade", "GTRF_FTYDWDK", balance);
//        insertData(SQL_DWDKJC, "GTRF_Core_Trade", "GTRF_FTYDWDK", base);
    }

    public void processCORPTDAC3(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        List<List<String>> dwckfs = new ArrayList<List<String>>();
        List<List<String>> tyckfs = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String DFSTUS = record.get("DFSTUS");
            if (DFSTUS == null || DFSTUS.equals("4") || DFSTUS.equals("5") ) {
                dwckjc.add(addWCASDWCKJC_CORPTDAC3(now, record));
                dwckye.add(addWCASDWCKYE_CORPTDAC3(now, record));
            } else {
                tyckjc.add(addWCASTYCKJC_CORPTDAC3(now, record));
                tyckye.add(addWCASTYCKYE_CORPTDAC3(now, record));
            }
            dwckfs.add(addWCASDWCKFS(now, record));
            tyckfs.add(addWCASTYCKFS(now, record));
        }
        insertData(SQL_DWCKJC, "OPS_WCAS","OPS_WCAS", dwckjc);
        insertData(SQL_DWCKYE, "OPS_WCAS", "OPS_WCAS", dwckye);
        insertData(SQL_TYCKJC, "OPS_WCAS", "OPS_WCAS", tyckjc);
        insertData(SQL_TYCKYE, "OPS_WCAS", "OPS_WCAS", tyckye);
        insertData(SQL_DWCKFS, "OPS_WCAS", "OPS_WCAS", dwckfs);
        insertData(SQL_TYCKFS, "OPS_WCAS", "OPS_WCAS", tyckfs);
//        insertData(SQL_DWDKFK, "GTRF_Core_Trade", "GTRF_FTYDWDK", occur);
//        insertData(SQL_DWDKYE, "GTRF_Core_Trade", "GTRF_FTYDWDK", balance);
//        insertData(SQL_DWDKJC, "GTRF_Core_Trade", "GTRF_FTYDWDK", base);
    }

    private List<String> addWCASDWCKJC_CORPDDAC(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("DFACB")+"-"+src.get("DFACS")+"-"+src.get("DFACX"));
        result.add("01");
        result.add(formatNBJGH(src.get("DFDCB")));
        result.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        result.add(src.get("DFAPTY"));
        result.add("");
        result.add(src.get("DFDTAO"));
        result.add("");
        result.add("");
        result.add("01");
        result.add("TR01");
        result.add("RF01");
        result.add("5.2");
        result.add("5.2");
        result.add("01");
        result.add("0");
        result.add("0");
        result.add("01");
        result.add("N");
        result.add("A");
        return result;
    }

    private List<String> addWCASDWCKJC_CORPTDAC3(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add("01");
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        result.add(src.get("TDAPTY"));
        result.add("");
        result.add(src.get("TDSTDT"));
        result.add("");
        result.add("");
        result.add("01");
        result.add("TR01");
        result.add("RF01");
        result.add("5.2");
        result.add("5.2");
        result.add("01");
        result.add("");
        result.add("");
        result.add("01");
        result.add("N");
        result.add("A");
        return result;
    }

    private List<String> addWCASTYCKJC_CORPDDAC(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("DFDCB")));
        result.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        result.add("A01");
        result.add(src.get("DFACB")+"-"+src.get("DFACS")+"-"+src.get("DFACX"));
        result.add("A01");
        result.add(src.get("DFDTAO"));
        result.add("");
        result.add("01");
        result.add("TR01");
        result.add("RF01");
        result.add("5.2");
        result.add("5.2");
        result.add("01");
        return result;
    }

    private List<String> addWCASTYCKJC_CORPTDAC3(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        result.add("A01");
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add("A01");
        result.add(src.get("TDSTDT"));
        result.add("");
        result.add("01");
        result.add("TR01");
        result.add("RF01");
        result.add("5.2");
        result.add("5.2");
        result.add("01");
        return result;
    }

    private List<String> addWCASDWCKYE_CORPDDAC(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("DFACB")+"-"+src.get("DFACS")+"-"+src.get("DFACX"));
        result.add("01");
        result.add(formatNBJGH(src.get("DFDCB")));
        result.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        result.add(src.get("DFCYCD"));
        result.add("0");
        return result;
    }

    private List<String> addWCASDWCKYE_CORPTDAC3(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add("01");
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        result.add(src.get("TDCYCD"));
        result.add("0");
        return result;
    }

    private List<String> addWCASTYCKYE_CORPDDAC(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("DFACB")+"-"+src.get("DFACS")+"-"+src.get("DFACX"));
        result.add(formatNBJGH(src.get("DFDCB")));
        result.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        result.add(src.get("DFCYCD"));
        result.add("0");
        return result;
    }

    private List<String> addWCASTYCKYE_CORPTDAC3(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        result.add(src.get("TDCYCD"));
        result.add("0");
        return result;
    }

    private List<String> addWCASDWCKFS(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add("01");
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        String THCPDT = src.get("THCPDT");
        String THCPWS = src.get("THCPWS");
        String THDLNO = src.get("THDLNO");
        while (THDLNO.length() < 5) {
            THDLNO = "0" + THDLNO;
        }
        result.add(THCPDT+THCPWS+THDLNO);
        result.add(now);
        result.add("5.2");
        result.add("5.2");
        result.add(src.get("TDCYCD"));
        result.add(src.get("LEDGER"));
        result.add("03");
        result.add("1");
        result.add("A");
        return result;
    }

    private List<String> addWCASTYCKFS(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("TDACB")+"-"+src.get("TDACS")+"-"+src.get("TDACX"));
        result.add(formatNBJGH(src.get("TDDCB")));
        result.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        String THCPDT = src.get("THCPDT");
        String THCPWS = src.get("THCPWS");
        String THDLNO = src.get("THDLNO");
        while (THDLNO.length() < 5) {
            THDLNO = "0" + THDLNO;
        }
        result.add(THCPDT+THCPWS+THDLNO);
        result.add(now);
        result.add(src.get("TDCYCD"));
        result.add("5.2");
        result.add("5.2");
        result.add(src.get("LEDGER"));
        result.add("1");
        return result;
    }

    public void processFTYDWDK(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("BILLREF");
            //保证金
            if (billref == null || billref.startsWith("M") || billref.startsWith("YM") ) {
                continue;
            }
            String productType = getMap("X21", billref.substring(0,3));
            if (productType.equals("同业")||productType.equals("票据贴现及转贴现")||productType.equals("GTE")||productType.equals(
                    "Standby DC")||productType.equals("DC")||productType.equals("BADI")) {
                continue;
            }
            String bbprcy = record.get("BBPRCY");
            if (bbprcy == null || !(bbprcy.startsWith("JPY") || bbprcy.startsWith("USD") || bbprcy.startsWith("EUR") || bbprcy.startsWith("HKD")) ) {
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
                if (obillref == null || obillref.startsWith("M") || billref.startsWith("YM") ) {
                    continue;
                }
                String oproductType = getMap("X21", obillref.substring(0,3));
                if (oproductType.equals("同业")||oproductType.equals("票据贴现及转贴现")||oproductType.equals("GTE")||oproductType.equals(
                        "Standby DC")||oproductType.equals("DC")||oproductType.equals("BADI")) {
                    continue;
                }
                String obbprcy = orecord.get("BBPRCY");
                if (obbprcy == null || !(obbprcy.startsWith("JPY") || obbprcy.startsWith("USD") || obbprcy.startsWith("EUR") || obbprcy.startsWith("HKD")) ) {
                    continue;
                }
                if (obillref.equals(billref)) {
                    find = true;
                    String badvos = orecord.get("ADVOS");
                    if (badvos == null || badvos.equals("0")) {
                        break;
                    }
                    if (new BigDecimal(advos).compareTo(new BigDecimal(badvos)) == 1) {
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
                base.add(addFtydwdkBase(now, record));
                balance.add(addFtydwdkBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addFtydwdkOccur(now, record));
            }
        }
        insertData(SQL_DWDKFK, "GTRF_Core_Trade", "GTRF_FTYDWDK", occur);
        insertData(SQL_DWDKYE, "GTRF_Core_Trade", "GTRF_FTYDWDK", balance);
        insertData(SQL_DWDKJC, "GTRF_Core_Trade", "GTRF_FTYDWDK", base);
    }

    public void processTYJD(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            String billref = record.get("BILLREF");
            if (billref == null || !(billref.startsWith("SBN") || billref.startsWith("MIR")) ) {
                continue;
            }
            String bbprcy = record.get("BBPRCY");
            if (bbprcy == null || !(bbprcy.startsWith("JPY") || bbprcy.startsWith("USD") || bbprcy.startsWith("EUR") || bbprcy.startsWith("HKD")) ) {
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
                if (billref == null || !(billref.startsWith("SBN") || billref.startsWith("MIR")) ) {
                    continue;
                }
                String obbprcy = orecord.get("BBPRCY");
                if (obbprcy == null || !(obbprcy.startsWith("JPY") || obbprcy.startsWith("USD") || obbprcy.startsWith("EUR") || obbprcy.startsWith("HKD")) ) {
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
                base.add(addTyjdBase(now, record));
                balance.add(addTyjdBalance(now, record));
            }
            if (matchOccur) {
                occur.add(addTyjdOccur(now, record));
            }
        }
        insertData(SQL_TYJDFS, "GTRF_Core_Trade", "GTRF_TYJD", occur);
        insertData(SQL_TYJDYE, "GTRF_Core_Trade", "GTRF_TYJD", balance);
        insertData(SQL_TYJDJC, "GTRF_Core_Trade", "GTRF_TYJD", base);
    }

    public void processPJTX(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        List<List<String>> balance = new ArrayList<List<String>>();
        List<List<String>> occur = new ArrayList<List<String>>();
       for (Map<String, String> record : lstNow) {
           String billref = record.get("BILLREF");
           if (billref == null || !(billref.startsWith("BBE") || billref.startsWith("XBG")|| billref.startsWith("FAO")|| billref.startsWith("FAW")|| billref.startsWith("FAT")|| billref.startsWith("BAT")|| billref.startsWith("BDP")|| billref.startsWith("DPF")|| billref.startsWith("BBS")) ) {
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
               if (obillref == null || !(obillref.startsWith("BBE") || obillref.startsWith("XBG")|| obillref.startsWith("FAO")|| obillref.startsWith("FAW")|| obillref.startsWith("FAT")|| obillref.startsWith("BAT")|| obillref.startsWith("BDP")|| obillref.startsWith("DPF")|| obillref.startsWith("BBS")) ) {
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
        insertData(SQL_PJTXFS, "GTRF_Core_Trade", "GTRF_PJTX", occur);
        insertData(SQL_PJTXYE, "GTRF_Core_Trade", "GTRF_PJTX", balance);
        insertData(SQL_PJTXJC, "GTRF_Core_Trade", "GTRF_PJTX", base);
////        BufferedWriter out = new BufferedWriter(new FileWriter("occur20210531.csv"));
////        for (List<String> record : occur) {
////            String s = "";
////            for (String field : record) {
////                s += field + ",";
////            }
////            s += "\n";
////            out.write(s);
////        }
//        out.close();
//        out = new BufferedWriter(new FileWriter("balance20210531.csv"));
//        for (List<String> record : balance) {
//            String s = "";
//            for (String field : record) {
//                s += field + ",";
//            }
//            s += "\n";
//            out.write(s);
//        }
//        out.close();
//        out = new BufferedWriter(new FileWriter("base20210531.csv"));
//        for (List<String> record : base) {
//            String s = "";
//            for (String field : record) {
//                s += field + ",";
//            }
//            s += "\n";
//            out.write(s);
//        }
//        out.close();
    }

    private void writeExcel(String fileName, List<List<String>> src) throws Exception{
        SXSSFWorkbook book = new SXSSFWorkbook(1000);
        Sheet sheet = book.createSheet("sheet1");
        Row row0 = sheet.createRow(0);
        FileInputStream in = new FileInputStream(fileName);
        try {
            Workbook wk = StreamingReader.builder()
                    .rowCacheSize(100)  //缓存到内存中的行数，默认是10
                    .bufferSize(4096)  //读取资源时，缓存到内存的字节大小，默认是1024
                    .open(in);  //打开资源，必须，可以是InputStream或者是File，注意：只能打开XLSX格式的文件
            Sheet sheet0 = wk.getSheetAt(0);
            for (Row row : sheet0) {
                if (row.getRowNum() == 0) {
                    int cnt = 0;
                    for (Cell cell : row) {
                        Cell cell0 = row0.createCell(cnt);
                        cell0.setCellValue(getCellValue(cell));
                        cnt++;
                    }
                }
            }

        } finally {
            in.close();
        }
        int rcnt = 1;
        for (List<String> record : src) {
            Row row1 = sheet.createRow(rcnt);
            rcnt++;
            int ccnt = 0;
            for (String field : record) {
                Cell cell1 = row1.createCell(ccnt);
                cell1.setCellValue(field);
                ccnt++;
            }
        }
        book.write(new FileOutputStream(fileName.substring(0, fileName.length()-5) + "_data.xlsx"));
    }

    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

    private void initMap() throws Exception {
        map.clear();
        String sql = "select * from map_info";
        statement = conn.createStatement();
        resultSet = statement.executeQuery(sql);
        List<Map<String, String>> lst = handle(resultSet);
        for (Map<String, String> record : lst) {
            Map<String, String> srecord = new HashMap<String, String>();
            if (map.containsKey(record.get("TYPE_NO"))) {
                srecord = map.get(record.get("TYPE_NO"));
            }
            srecord.put(record.get("SRC"), record.get("DEST"));
            map.put(record.get("TYPE_NO"), srecord);
        }
        sql = "select NBJGH, DQDM from imas_pm_jgfzxx where sjrq = '20210531'";
        resultSet = statement.executeQuery(sql);
        lst = handle(resultSet);
        Map<String, String> dqdm = new HashMap<String, String>();
        for (Map<String, String> record : lst) {
            dqdm.put(record.get("NBJGH"), record.get("DQDM"));
        }
        map.put("XDQDM", dqdm);
        sql = "select DATA_NO from gp_bm_data_dic where data_type_no = 'C_REGION_CODE'";
        resultSet = statement.executeQuery(sql);
        lst = handle(resultSet);
        Map<String, String> dqqhdm = new HashMap<String, String>();
        for (Map<String, String> record : lst) {
            dqqhdm.put(record.get("DATA_NO"), record.get("DATA_NO"));
        }
        map.put("DQQHDM", dqqhdm);
    }

    public boolean process(String type, String now, String previous) throws Exception {
        try {
            if (conn == null) {
                Driver driver = new com.mysql.cj.jdbc.Driver();
                DriverManager.deregisterDriver(driver);
                Properties pro = new Properties();
                pro.put("user", properties.getProperty("jdbc.username"));
                pro.put("password", properties.getProperty("jdbc.password"));
                conn = driver.connect(properties.getProperty("jdbc.url"), pro);
            }
        } catch (Exception ex) {
            System.out.println("无法连接数据库");
            return false;
        }
        initMap();
        String sql = "select * from " + type + " where data_date = '" + now + "'";
        statement = conn.createStatement();
        resultSet = statement.executeQuery(sql);
        List<Map<String, String>> lst = handle(resultSet);
        sql = "select * from " + type + " where data_date = '" + previous + "'";
        resultSet = statement.executeQuery(sql);
        List<Map<String, String>> lst1 = handle(resultSet);
        if (type.equals("GTRF_PJTX")) {
            processPJTX(now, lst, lst1);
        } else if (type.equals("GTRF_TYJD")) {
            processTYJD(now, lst, lst1);
        } else if (type.equals("GTRF_FTYDWDK")) {
            processFTYDWDK(now, lst, lst1);
        } else if (type.equals("GTRF_FTYSCSAI")) {
            processFTYSCSAI(now, lst, lst1);
        } else if (type.equals("GTRF_GRKHXX_BOSC")) {
            processGRKHXXBASE(now, lst, lst1);
        } else if (type.equals("GTRF_GRKHXX")) {
            processGRKHXX(now, lst, lst1);
        } else if (type.equals("GTRF_CORPCUSLVL")) {
            processWCAS_DGHKXX(now, lst, lst1);
        } else if (type.equals("GTRF_CORPDDAC")) {
            processCORPDDAC(now, lst, lst1);
        } else if (type.equals("GTRF_CORPTDAC3")) {
            processCORPTDAC3(now, lst, lst1);
        }
        return true;
    }

    public List<Map<String, String>> handle(ResultSet set) throws SQLException {
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();

        ResultSetMetaData rsmd =set.getMetaData();
        int count = rsmd.getColumnCount();
        while(set.next()){
            Map<String,String> record = new HashMap<String,String>();
            for(int i=0 ;i<count;i++){
                record.put(rsmd.getColumnName(i+1), set.getString(rsmd.getColumnName(i+1)));
            }
            result.add(record);
        }
        return result;
    }

    private void printDict(String type, String type_no, String type_value) {
        System.out.println(String.format("insert into MAP_INFO(data_id,type_no,src,dest,system)values('%s'," +
                "'%s','%s','%s','%s');", type+type_no, type, type_no, type_value,"GTRF"));
    }

    public void createMap() throws Exception {
        System.out.println("delete from MAP_INFO;");
        Workbook wb = new XSSFWorkbook(new FileInputStream("票据贴现.xlsx"));
        Sheet st = wb.getSheet("mapping");
        for (int i = 1; i < 10; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                printDict("X1", type_no, type_value);
            }
        }
        for (int i = 1; i < 10; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(3));
                printDict("X2", type_no, type_value);
            }
        }
        for (int i = 12; i < 15; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                printDict("X3", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("同业借贷.xlsx"));
        st = wb.getSheet("mapping");
        for (int i = 3; i < 136; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                printDict("X11", type_no, type_value);
            }
        }
        for (int i = 2; i < 16; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(3));
                String type_value = getCellValue(row.getCell(4));
                printDict("X12", type_no, type_value);
            }
        }
        for (int i = 2; i < 6; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(6));
                String type_value = getCellValue(row.getCell(7));
                printDict("X13", type_no, type_value);
            }
        }
        for (int i = 1; i < 16; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(10));
                String type_value = getCellValue(row.getCell(11));
                printDict("X14", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("非同业单位贷款.xlsx"));
        st = wb.getSheet("mapping");
        for (int i = 1; i < 36; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                printDict("X21", type_no, type_value);
            }
        }
        for (int i = 1; i < 15; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(3));
                String type_value = getCellValue(row.getCell(4));
                printDict("X22", type_no, type_value);
            }
        }
        for (int i = 1; i < 17; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(7));
                String type_value = getCellValue(row.getCell(8));
                printDict("X23", type_no, type_value);
            }
        }
        for (int i = 1; i < 472; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(10));
                String type_value = getCellValue(row.getCell(12));
                printDict("X24", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("非同业-scsai.xlsx"));
        st = wb.getSheet("mapping");
        for (int i = 1; i < 116; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                printDict("X31", type_no, type_value);
            }
        }
        for (int i = 1; i < 17; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(4));
                String type_value = getCellValue(row.getCell(5));
                printDict("X32", type_no, type_value);
            }
        }
        for (int i = 1; i < 15; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(7));
                String type_value = getCellValue(row.getCell(8));
                printDict("X33", type_no, type_value);
            }
        }
        for (int i = 1; i < 164; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(10));
                String type_value = getCellValue(row.getCell(11));
                printDict("X34", type_no, type_value);
            }
        }
        for (int i = 1; i < 90; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(13));
                String type_value = getCellValue(row.getCell(14));
                printDict("X35", type_no, type_value);
            }
        }
        for (int i = 1; i < 472; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(17));
                String type_value = getCellValue(row.getCell(18));
                printDict("X36", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("CB_Code.xlsx"));
        st = wb.getSheet("CB Code");
        for (int i = 1; i < 139; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(6));
                String type_value = getCellValue(row.getCell(7));
                String type_value1 = getCellValue(row.getCell(8));
                String type_value2 = getCellValue(row.getCell(9));
                String type_value3 = getCellValue(row.getCell(10));
                printDict("X41", type_no, type_value);
                printDict("X42", type_no, type_value1);
                printDict("X43", type_no, type_value2);
                printDict("WCAS_TYBZ", type_no, type_value3);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("FinData.xlsx"));
        st = wb.getSheetAt(0);
        for (int i = 1; i < 3901; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                String type_value1 = getCellValue(row.getCell(2));
                printDict("X44", type_no, type_value);
                printDict("X45", type_no, type_value1);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("ProductType.xlsx"));
        st = wb.getSheetAt(0);
        for (int i = 1; i < 36; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(5));
                printDict("WCAS_ProductType", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("IndustryCode.xlsx"));
        st = wb.getSheetAt(0);
        for (int i = 5; i < 1882; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(6));
                String type_value = getCellValue(row.getCell(7));
                if (!type_no.trim().equals("") && type_no.length() > 1)
                printDict("X46", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("RateType.xlsx"));
        st = wb.getSheetAt(0);
        for (int i = 1; i < 614; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String id1 = getCellValue(row.getCell(1));
                String id2 = getCellValue(row.getCell(2));
                String id3 = getCellValue(row.getCell(3));
                String id4 = getCellValue(row.getCell(12));
                String DJJZLX = getCellValue(row.getCell(13));
                String LVLX = getCellValue(row.getCell(14));
                String JZLV = getCellValue(row.getCell(18));
                String LVFDPV = getCellValue(row.getCell(19));
                if (id4.equals("DD") || id4.equals("TD")) {
                    System.out.println(String.format("insert into MAP_WCAS_RATE_TYPE(id1,id2,id3,DJJZLX,LVLX,JZLV," +
                            "LVFDPL)values('%s','%s','%s','%s','%s','%s','%s');",id1,id2,id3,DJJZLX,LVLX,JZLV,LVFDPV));
                }
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("TermCode.xlsx"));
        st = wb.getSheetAt(0);
        for (int i = 1; i < 10; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(1));
                if (!type_no.trim().equals("") && type_no.length() > 1)
                    printDict("WCAS_TERMCODE_FIX", type_no, type_value);
            }
        }

        for (int i = 1; i < 16; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(3));
                String type_value = getCellValue(row.getCell(4));
                if (!type_no.trim().equals("") && type_no.length() > 1)
                    printDict("WCAS_TERMCODE", type_no, type_value);
            }
        }
        wb.close();
        wb = new XSSFWorkbook(new FileInputStream("WPB.xlsx"));
        st = wb.getSheetAt(6);
        for (int i = 8; i < 109; i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(0));
                String type_value = getCellValue(row.getCell(10));
                if (!type_no.trim().equals("") && type_no.length() > 1) {
                    if (type_value.indexOf("：") > 0) {
                        type_value = type_value.substring(0, type_value.indexOf("："));
                    }
                    type_value = type_value.trim();
                    if (type_value.length() > 5) {
                        type_value = type_value.substring(0,5);
                    }
                    printDict("WPB_CKCPLB", type_no, type_value);
                }
            }
        }

//        wb.close();
//        wb = new XSSFWorkbook(new FileInputStream("NBJGH.xlsx"));
//        st = wb.getSheetAt(0);
//        for (int i = 1; i < 202; i++) {
//            Row row = st.getRow(i);
//            if (row != null) {
//                String type_no = getCellValue(row.getCell(0));
//                String type_value = getCellValue(row.getCell(1));
//                System.out.println(String.format("insert into map_nbjgh(`id`,`src`,`dest`)values('%s'," +
//                        "'%s','%s');", String.valueOf(i), type_no, type_value));
//            }
//        }
    }

    public void test() throws Exception {
        List<List<String>> n1 = new ArrayList<List<String>>();
        List<String> nn1 = new ArrayList<String>();
        nn1.add("20210531");
        nn1.add("2");
        nn1.add("1");
        nn1.add("2");
        nn1.add("2");
        nn1.add("1");
        nn1.add("2");
        nn1.add("2");
        nn1.add("1");
        nn1.add("2");
//        nn1.add("1");
//        nn1.add("1");
//        nn1.add("2");
//        nn1.add("1");
//        nn1.add("1");
//        nn1.add("2");
//        nn1.add("1");
//        nn1.add("2");
        n1.add(nn1);
        List<String> nn2 = new ArrayList<String>();
        nn2.add("20210531");
        nn2.add("3");
        nn2.add("4");
        nn2.add("5");
        nn2.add("6");
        nn2.add("7");
        nn2.add("12");
        nn2.add("12");
        nn2.add("11");
        nn2.add("3");
//        nn2.add("4");
//        nn2.add("3");
//        nn2.add("4");
//        nn2.add("3");
//        nn2.add("4");
//        nn2.add("3");
//        nn2.add("4");
//        nn2.add("3");
        n1.add(nn2);
//        writeExcel("ExcelTemplate_票据贴现及转贴现发生额信息表补录_HSBC_HSBC01.xlsx", n1);
        insertData(SQL_PJTXFS, "T1 T2","T3", n1);
    }

    public boolean check(List<List<String>> base, List<String> record) {
        base.get(0).set(0, "333");
        return false;
    }
    public void test1(String dir) {
        File file = new File(dir);
        File[] files = file.listFiles();
        String s = "javac -cp .;";
        for (int i = 0; i < files.length; i++) {
            if (files[i].getName().toUpperCase().endsWith(".JAR")) {
                s += files[i].getAbsolutePath().replace("\\","/") + ";";
            }
        }
        String s1 = s.substring(0, s.length()-1) + " com/gingkoo/imas/hsbc/service/CustLoadFileService.java";
        System.out.println(s1);
        s1 = s.substring(0, s.length()-1) + " com/gingkoo/imas/hsbc/service/CustValidateService.java";
        System.out.println(s1);
    }

    public void testExcel(String dir) throws Exception {
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        System.out.println("Row:"+st.getLastRowNum());
        System.out.println(getCellValue(st.getRow(0).getCell(0)));
        Row row = st.getRow(1);
        for (int i = 0; i <= row.getLastCellNum(); i++) {
            System.out.print(getCellValue(row.getCell(i)) + ",");
        }
    }

    public void createNBJGH(String dir) throws Exception {
        String date = dir.substring(dir.indexOf(".")-10, dir.indexOf(".")-3).replace("-","");
        Workbook wb = new XSSFWorkbook(new FileInputStream(dir));
        Sheet st = wb.getSheetAt(0);
        Map<String,String> dict = new HashMap<String,String>();
        for (int i = 1; i <= st.getLastRowNum(); i++) {
            Row row = st.getRow(i);
            if (row != null) {
                String type_no = getCellValue(row.getCell(1));
                String type_value = getCellValue(row.getCell(2));
                dict.put(type_no, type_value);
            }
        }
        for (int i = 1; i< 32; i++) {
            String x = String.valueOf(i);
            if (x.length() == 1) {
                x = "0" + x;
            }
            System.out.println("delete from map_nbjgh where data_date = '"+date + x +"';");
            for (String key : dict.keySet()) {
                String id = UUID.randomUUID().toString().replace("-", "");
                String data_date = date + x;
                System.out.println(String.format("insert into map_nbjgh(id,src,dest,data_date)values('%s','%s','%s'," +
                        "'%s');", id, key, dict.get(key), data_date));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //System.out.println(args[0]);
        if (args.length == 0) {
            System.out.println("C - create table with giving excelname");

            return;
        }
        String mode = args[0];
        if (mode.equals("C")) {
            String filename = args[1];
            String tablename = args[2];
            String pk = args[3];
            MainApplication t = new MainApplication();
            t.printCreateSql(filename, tablename, pk);

        /*
        if (!new File(filename).exists()) {
            System.out.println("File not exists");
            return;
        }
        if (!filename.toUpperCase().endsWith(".XLSX")) {
            System.out.println("File type not support");
            return;
        }
        */
        } else if (mode.equals("I")) {
            String filename = args[1];
            String data_date = args[2];
            MainApplication t = new MainApplication();
            t.loadProperties();
            String tablename = t.parseTableName(filename);
            if (tablename.equals("")) {
                System.out.println("无法从文件名定位表名，检查application.properties中的映射");
                return;
            }
            t.loadExcel(filename, tablename, data_date, 7);
            t.clearConnect();
        }  else if (mode.equals("II")) {
            String filename = args[1];
            String data_date = args[2];
            MainApplication t = new MainApplication();
            t.loadProperties();
            String tablename = t.parseTableName(filename);
            if (tablename.equals("")) {
                System.out.println("无法从文件名定位表名，检查application.properties中的映射");
                return;
            }
            t.loadExcel(filename, tablename, data_date, 7);
            t.clearConnect();
        } else if (mode.equals("P")) {
            String type = args[1];
            String now = args[2];
            String previous = args[3];
            MainApplication t = new MainApplication();
            t.loadProperties();
            t.process(type, now, previous);
        } else if (mode.equals("M")) {
            MainApplication t = new MainApplication();
            t.loadProperties();
            t.createMap();
        } else if (mode.equals("COMPILE")) {
            MainApplication t = new MainApplication();
            t.loadProperties();
            String dir = args[1];
            t.test1(dir);
        } else if (mode.equals("NBJGH")) {
            MainApplication t = new MainApplication();
            t.loadProperties();
            String dir = args[1];
            t.createNBJGH(dir);
        } else if (mode.equals("T")) {
            MainApplication t = new MainApplication();
            t.loadProperties();
            String dir = args[1];
            t.testExcel(dir);
        }

    }
}
