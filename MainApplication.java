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
import sun.java2d.pipe.SpanShapeRenderer.Simple;

public class MainApplication {

    private static final Logger log = LoggerFactory.getLogger(MainApplication.class);

    public static int batchCount = 3;

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

    public void loadExcel(String filename, String tablename, String data_date) throws Exception {
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
                    sql = "insert into " + tablename + "(" + columns + ",data_date) values (";

                    puresql = sql;
                    for (int k = 0; k < columncnt; k++) {
                        sql += "?,";
                    }
                    sql = sql.substring(0, sql.length() - 1) + ",'"+data_date+"')";
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
                    s+= "`DATA_DATE` VARCHAR(8),\nPRIMARY KEY (`"+key+"`) )";
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
        result.add(src.get("C3CUNO"));
        result.add(getMap("X31", src.get("C3CUNO")));
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
        result.add("");
        result.add(src.get("C3INMG"));
        result.add("");
        String purposeCode = getMap("X35", src.get("C3CUNO"));
        if (purposeCode != null && purposeCode.length() >= 3) {
            result.add(getMap("X36", purposeCode.substring(0, 3)));
        } else {
            System.out.println("No PurposeCode:>>>" + src.get("C3BLRF"));
            result.add("");
        }
        result.add("01");
        result.add("");
        return result;
    }

    private List<String> addFtyscsaiBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("C3BLRF"));
        result.add(src.get("C3CUNO"));
        result.add(getMap("X31", src.get("C3CUNO")));
        result.add(src.get("BBPRCY"));
        result.add(src.get("C3INVA"));
        return result;
    }

    private List<String> addFtyscsaiOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("C3BLRF"));
        result.add(src.get("C3CUNO"));
        result.add(getMap("X31", src.get("C3CUNO")));


        result.add(src.get("C3BLRF")+src.get("交易方向"));
        result.add(src.get("C3RCDT"));
        result.add(src.get("C3CYCD"));
        result.add(src.get("C3BLAM"));
        result.add("");
        String bbdrsp = src.get("C3INMG");
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
        result.add(src.get("CUS"));
        result.add(src.get("ACCOUNTNO").substring(0,3));
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
        return result;
    }

    private List<String> addFtydwdkBalance(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(src.get("CUS"));
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addFtydwdkOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(src.get("CUS"));
        result.add(src.get("ACCOUNTNO").substring(0,3));


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
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("CUS"));
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
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("CUS"));
        result.add(src.get("BBPRCY"));
        result.add(formatJPY(src.get("BBPRCY"),src.get("ADVOS")));
        return result;
    }

    private List<String> addTyjdOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("BILLREF"));
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("CUS"));

        result.add(src.get("BILLREF")+src.get("交易方向"));
        result.add(now);
        result.add(src.get("BBPRCY"));
        result.add(src.get("BBDRSP"));
        result.add(src.get("BILLAMT"));
        result.add(src.get("交易方向"));
        return result;
    }

    private List<String> addBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("ACCOUNTNO"));


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
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("ACCOUNTNO"));
        result.add(src.get("BILLREF"));
        result.add(src.get("BBPRCY"));
        result.add(src.get("ADVOS"));
        return result;
    }

    private List<String> addOccur(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("ACCOUNTNO"));
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
        writeExcel("ExcelTemplate_非同业单位贷款放款信息表补录_HSBC_HSBC011.xlsx", occur);
        writeExcel("ExcelTemplate_非同业单位贷款余额信息表补录_HSBC_HSBC011.xlsx", balance);
        writeExcel("ExcelTemplate_非同业单位贷款基础信息表补录_HSBC_HSBC011.xlsx", base);
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
        writeExcel("ExcelTemplate_非同业单位贷款放款信息表补录_HSBC_HSBC01.xlsx", occur);
        writeExcel("ExcelTemplate_非同业单位贷款余额信息表补录_HSBC_HSBC01.xlsx", balance);
        writeExcel("ExcelTemplate_非同业单位贷款基础信息表补录_HSBC_HSBC01.xlsx", base);
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
        writeExcel("ExcelTemplate_同业借贷发生额信息表补录_HSBC_HSBC01.xlsx", occur);
        writeExcel("ExcelTemplate_同业借贷余额信息表补录_HSBC_HSBC01.xlsx", balance);
        writeExcel("ExcelTemplate_同业借贷基础信息表补录_HSBC_HSBC01.xlsx", base);
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
       writeExcel("ExcelTemplate_票据贴现及转贴现发生额信息表补录_HSBC_HSBC01.xlsx", occur);
       writeExcel("ExcelTemplate_票据贴现及转贴现余额信息表补录_HSBC_HSBC01.xlsx", balance);
       writeExcel("ExcelTemplate_票据贴现及转贴现基础信息表补录_HSBC_HSBC01.xlsx", base);
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
                String type_no = getCellValue(row.getCell(11));
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
    }

    public void test() throws Exception {
        List<List<String>> n1 = new ArrayList<List<String>>();
        List<String> nn1 = new ArrayList<String>();
        nn1.add("01");
        nn1.add("02");
        n1.add(nn1);
        writeExcel("ExcelTemplate_票据贴现及转贴现发生额信息表补录_HSBC_HSBC01.xlsx", n1);
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
            t.loadExcel(filename, tablename, data_date);
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
        } else if (mode.equals("T")) {
            MainApplication t = new MainApplication();
            t.test();
        }

    }
}
