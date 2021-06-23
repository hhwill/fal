import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
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

    private List<String> addBase(String now, Map<String, String> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(src.get("ACCOUNTNO").substring(0,3));
        result.add(src.get("ACCOUNTNO"));

        result.add(map.get("X2").get(src.get("ACCOUNTNO").substring(0,3)));
        result.add(src.get("BILLREF"));
        result.add(map.get("X1").get(src.get("ACCOUNTNO").substring(0,3)));
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

    public void processPJTX(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious) throws Exception {
        System.out.println(lstPrevious.size());
        /*
        for (Map<String, String> record : lstPrevious) {
            String billref = record.get("BILLREF");
            if (billref == null || !(billref.startsWith("BBE") || billref.startsWith("XBG")|| billref.startsWith("FAO")|| billref.startsWith("FAW")|| billref.startsWith("FAT")|| billref.startsWith("BAT")|| billref.startsWith("BDP")|| billref.startsWith("DPF")|| billref.startsWith("BBS")) ) {
                lstPrevious.remove(record);
            }
            String ADVOS = record.get("ADVOS");
            if (ADVOS.equals("0")) {
                lstPrevious.remove(record);
                continue;
            }
        }
        */
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
        BufferedWriter out = new BufferedWriter(new FileWriter("occur20210531.csv"));
        for (List<String> record : occur) {
            String s = "";
            for (String field : record) {
                s += field + ",";
            }
            s += "\n";
            out.write(s);
        }
        out.close();
        out = new BufferedWriter(new FileWriter("balance20210531.csv"));
        for (List<String> record : balance) {
            String s = "";
            for (String field : record) {
                s += field + ",";
            }
            s += "\n";
            out.write(s);
        }
        out.close();
        out = new BufferedWriter(new FileWriter("base20210531.csv"));
        for (List<String> record : base) {
            String s = "";
            for (String field : record) {
                s += field + ",";
            }
            s += "\n";
            out.write(s);
        }
        out.close();
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
            srecord.put(record.get("src"), record.get("dest"));
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
        processPJTX(now, lst, lst1);
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
        }

    }
}
