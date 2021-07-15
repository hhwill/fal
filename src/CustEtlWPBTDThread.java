package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

public class CustEtlWPBTDThread implements Callable<List<List<List<String>>>> {

    private final Logger logger = LoggerFactory.getLogger(CustEtlWPBTDThread.class);

    private final JdbcTemplate jdbcTemplate;

    private Integer pageSize;

    private Integer pageIndex;

    private String sql;

    private String rptDate;

    public CustEtlWPBTDThread(DataSource dataSource, String sql, int pageSize, int pageIndex,
                              String rptDate) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.pageSize = pageSize;
        this.pageIndex = pageIndex;
        this.sql = sql;
        this.rptDate = rptDate;
    }

    @Override
    public List<List<List<String>>> call() {
        List<List<List<String>>> result = new ArrayList<List<List<String>>>();
        List<List<String>> grckjc = new ArrayList<List<String>>();
        List<List<String>> grckye = new ArrayList<List<String>>();
        List<List<String>> grckfs = new ArrayList<List<String>>();
        String mysql = String.format(sql + " limit %d, %d" , pageIndex * pageSize, pageSize);
        List<Map<String, Object>> records = jdbcTemplate.queryForList(mysql);
        for (Map<String, Object> src : records) {
            String ZGCUCL = getString(src.get("ZGCUCL"));
//            if (!ZGCUCL.equals("OPR") ||!ZGCUCL.equals("RCC") ||!ZGCUCL.equals("SJL")
//                    ||!ZGCUCL.equals("SSL") ||!ZGCUCL.equals("STL") ||!ZGCUCL.equals("STS") ) {
//                continue;
//            }
            String TDAPTY = getString(src.get("TDAPTY"));
            String group_id = CustEtlWPB.WPB_TEAM_DEF.get("TD");
            if (TDAPTY.equals("D41")) {
                group_id = CustEtlWPB.WPB_TEAM_DEF.get("EYI");
            } else if (TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                group_id = CustEtlWPB.WPB_TEAM_DEF.get("CPI");
            } else if (TDAPTY.equals("D01") || TDAPTY.equals("D02") ||
                    TDAPTY.equals("D03") || TDAPTY.equals("D04")) {
                group_id = CustEtlWPB.WPB_TEAM_DEF.get("DCI");
            }
            String TDTERM = getString(src.get("TDTERM"));
            String type = "";
            if (TDAPTY.equals("TDI") || TDAPTY.equals("TD1") || TDAPTY.equals("TD2") || TDAPTY.equals("TD4") ||
                    TDAPTY.equals("TD9") ||TDAPTY.equals("TMD") ||TDAPTY.equals("TD5") ||TDAPTY.equals("CD4") ) {
                type = "DQCK";
            } else if (TDAPTY.equals("D01") || TDAPTY.equals("D02") || TDAPTY.equals("D03") || TDAPTY.equals("D04") ||
                    TDAPTY.equals("D41") ||TDAPTY.equals("D42") ||TDAPTY.equals("D43") ) {
                type = "JGXCK";
            }
            if (type.equals("")) {
                continue;
            } else if (type.equals("JGXCK")) {
                String TDSTDT = getString(src.get("TDSTDT"));
                if (!TDSTDT.equals("")) {
                    try {
                        if (Integer.parseInt(TDSTDT) > Integer.parseInt(rptDate)) {
                            continue;
                        }
                    } catch (Exception ex) {

                    }
                }
            }
            List<List<String>> ckxx = getCKXH(src);
            List<String> subgrckjc = new ArrayList<String>();
            List<String> subgrckye = new ArrayList<String>();
            subgrckjc.add(rptDate);
            subgrckjc.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
            subgrckjc.add("01");
            subgrckjc.add(formatNBJGH(src.get("TDACB")));
            subgrckjc.add(getString(src.get("CUS")));
            subgrckjc.add(getMap("WPB_CKCPLB", src.get("TDAPTY")));
            subgrckjc.add(getString(src.get("TDSTDT")));
            //8-到期日期
            if (type.equals("DQCK")) {
                if (TDTERM.equals("0000")) {
                    subgrckjc.add("19990101");
                } else if (TDTERM.equals("7D")) {
                    subgrckjc.add("19990107");
                } else {
                    subgrckjc.add(getString(src.get("TDDUDT")));
                }
            } else {
                subgrckjc.add(getString(src.get("TDDUDT")));
            }
            //9-实际终止日期
            if (getString(src.get("TDSTUS")).equals("5")) {
                if (type.equals("DQCK")) {
                    subgrckjc.add(getMap("WPB_CLOSEDAC",
                            getString(src.get("TDACB")) + getString(src.get("TDACS")) +
                                    getString(src.get("TDACX") + getString(src.get("TDCYCD")))));
                } else {
                    subgrckjc.add(getString(src.get("TDDUDT")));
                }
            } else {
                subgrckjc.add("");
            }
            //存款期限类型
            String CKQXLX = "";

            CKQXLX =checkWcasTendor(TDTERM, map.get("WCAS_TERMCODE"));
            if (TDTERM.equals("7D")) {
                CKQXLX = "02";
            } else if (TDTERM.equals("1M")) {
                CKQXLX = "03";
            } else if (TDTERM.equals("3M")) {
                CKQXLX = "05";
            } else if (TDTERM.equals("6M")) {
                CKQXLX = "07";
            } else if (TDTERM.equals("12M")) {
                CKQXLX = "09";
            } else if (TDTERM.equals("24M")) {
                CKQXLX = "11";
            } else if (TDTERM.equals("36M")) {
                CKQXLX = "13";
            } else if (TDTERM.equals("60M")) {
                CKQXLX = "15";
            }
            if (type.equals("DQCK")) {
            } else {
                int days = differentDaysByMillisecond(src.get("TDDUDT"), src.get("TDSTUS"));
                if (TDTERM.equals("0000") && (TDAPTY.equals("D01") ||TDAPTY.equals("D02") ||TDAPTY.equals("D03")
                        ||TDAPTY.equals("D04"))) {
                    CKQXLX =checkWcasTendor(String.valueOf(days), map.get("WCAS_TERMCODE"));
                } else if (TDAPTY.equals("D41") || TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                } else {
                    CKQXLX = "";
                }
            }
            subgrckjc.add(CKQXLX);
            if (type.equals("DQCK")) {
                subgrckjc.add("TR07");
            } else {
                subgrckjc.add("TR99");
            }
            //12-利率类型
            if (type.equals("DQCK")) {
                subgrckjc.add("RF01");
            } else {
                subgrckjc.add("RF02");
            }
            //13-实际利率
            if (type.equals("DQCK")) {
                subgrckjc.add(getString(src.get("TDCNTR")));
            } else {
                String SJLL = "";
                if (TDAPTY.equals("D41")) {
                    SJLL = getString(src.get("EMCIRS"));
                }else if (TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                    SJLL = getMap("WPB_BDSYL", src.get("TDTRNR"));
                    if (SJLL.startsWith("-")) {
                        SJLL = "";
                    }
                }
                subgrckjc.add(SJLL);
            }
            //基准利率
            subgrckjc.add("");
            //15-利率浮动频率
            if (type.equals("DQCK")) {
                subgrckjc.add("");
            } else {
                subgrckjc.add("99");
            }
            //16保底收益率
            if (type.equals("DQCK")) {
                subgrckjc.add("");
            } else {
                if (TDAPTY.equals("D41")) {
                    subgrckjc.add(getString(src.get("EMCIRS")));
                } else if (TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                    String BDSYL = getMap("WPB_BDSYL", src.get("TDTRNR"));
                    if (BDSYL.startsWith("-")) {
                        BDSYL = "";
                    }
                    subgrckjc.add(BDSYL);
                } else {
                    subgrckjc.add("");
                }
            }
            //17最高收益率
            if (type.equals("DQCK")) {
                subgrckjc.add("");
            } else {
                String ZGSYL = "";
                if (TDAPTY.equals("D41")) {
                    ZGSYL = getString(src.get("EMCIRS"));
                } else if (TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                    String TDTRNR = getString(src.get("TDTRNR"));
                    if (TDTRNR.startsWith("DA") || TDTRNR.startsWith("DC") || TDTRNR.startsWith("DP") ||
                            TDTRNR.startsWith("TDC") ||
                            TDTRNR.startsWith("TDP") ||
                            TDTRNR.startsWith("AC") ||
                            TDTRNR.startsWith("GR")) {
                        ZGSYL = getMap("WPB_ZGSYL", src.get("TDTRNR"));
                    }
                }
                subgrckjc.add(ZGSYL);
            }
            //开户渠道
            if (type.equals("DQCK")) {
                if (getString(src.get("TDMIN1")).contains("IB")) {
                    subgrckjc.add("02");
                } else {
                    subgrckjc.add("01");
                }
            } else {
                if (TDAPTY.equals("D41") || ((TDAPTY.equals("D42") || TDAPTY.equals("D43")) &&
                        getString(src.get("TDMIN3")).equals("")) ||
                        ((TDAPTY.equals("D01") || TDAPTY.equals("D02")||TDAPTY.equals("D03") || TDAPTY.equals("D04"))
                                && getString(src.get("TDCHID")).equals("OHB")) ) {
                    subgrckjc.add("01");
                } else if (((TDAPTY.equals("D01") || TDAPTY.equals("D02")||TDAPTY.equals("D03") || TDAPTY.equals("D04"))
                        && getString(src.get("TDCHID")).equals("OHI")) ||
                        (TDAPTY.equals("D42") || TDAPTY.equals("D43")) || !(getString(src.get("TDMIN3")).equals(""))) {
                    subgrckjc.add("02");
                } else {
                    subgrckjc.add("");
                }
            }
            subgrckjc.add("N");
            subgrckjc.add(getDXEBZ(src.get("TDCYCD"), src.get("LEDGER")));
            subgrckjc.add(group_id);
            grckjc.add(subgrckjc);

            subgrckye.add(rptDate);
            subgrckye.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
            subgrckye.add("01");
            subgrckye.add(formatNBJGH(src.get("TDACB")));
            subgrckye.add(getString(src.get("CUS")));
            subgrckye.add(getString(src.get("TDCYCD")));
            subgrckye.add(formatJPY(src.get("TDCYCD"), src.get("LEDGER")));
            subgrckye.add(group_id);
            grckye.add(subgrckye);

            List<List<String>> jyls = getWCASJYLS(rptDate, src);
            //for (List<String> subjyls : jyls) {
            List<String> subgrckfs = new ArrayList<String>();
            subgrckfs.add(rptDate);
            subgrckfs.add(formatCKZHBH(src.get("TDACB"), src.get("TDACS"), src.get("TDACX")));
            subgrckfs.add("01");
            subgrckfs.add(formatNBJGH(src.get("TDACB")));
            subgrckfs.add(getString(src.get("CUS")));
            //交易流水号
            subgrckfs.add(formatCKZHBH(src.get("THCPDT"), src.get("THCPWS"), src.get("THDLNO")));
            subgrckfs.add(getString(src.get("THCPDT")));
            subgrckfs.add("");
            //实际利率
            if (type.equals("DQCK")) {
                subgrckfs.add(getString(src.get("TDCNTR")));
            } else {
                String SJLL = "";
                if (TDAPTY.equals("D41")) {
                    SJLL = getString(src.get("EMCIRS"));
                }else if (TDAPTY.equals("D42") || TDAPTY.equals("D43")) {
                    SJLL = getMap("WPB_BDSYL", src.get("TDTRNR"));
                    if (SJLL.startsWith("-")) {
                        SJLL = "";
                    }
                }
                subgrckfs.add(SJLL);
            }
            subgrckfs.add(getString(src.get("TDCYCD")));
            String TRANAMT = getString(src.get("TRANAMT"));
            if (TRANAMT.startsWith("-")) {
                TRANAMT = TRANAMT.substring(1);
            }
            subgrckfs.add(TRANAMT);
            //交易渠道
            String JYQD = "";
            if (type.equals("DQCK")) {
                if (getString(src.get("TDMIN1")).startsWith("HIB")) {
                    JYQD = "04";
                } else {
                    JYQD = "01";
                }
            } else {
                if (TDAPTY.equals("D41") || ((TDAPTY.equals("D42") || TDAPTY.equals("D43")) &&
                        getString(src.get("TDMIN3")).equals("")) ||
                        ((TDAPTY.equals("D01") || TDAPTY.equals("D02") || TDAPTY.equals("D03") || TDAPTY.equals("D04"))
                                && getString(src.get("TDCHID")).equals("OHB"))) {
                    JYQD = "01";
                } else if (((TDAPTY.equals("D01") || TDAPTY.equals("D02") || TDAPTY.equals("D03") || TDAPTY.equals("D04"))
                        && getString(src.get("TDCHID")).equals("OHI")) ||
                        ((TDAPTY.equals("D42") || TDAPTY.equals("D43")) && getString(src.get("TDMIN3")).equals("NET"))) {
                    JYQD = "03";
                } else if (((TDAPTY.equals("D01") || TDAPTY.equals("D02") || TDAPTY.equals("D03") || TDAPTY.equals("D04"))
                        && getString(src.get("TDCHID")).equals("MOB")) ||
                        ((TDAPTY.equals("D42") || TDAPTY.equals("D43")) && getString(src.get("TDMIN3")).equals(
                                "Mobile"))) {
                    JYQD = "04";
                }
            }
            subgrckfs.add(JYQD);
            if (new BigDecimal(getString(src.get("TRANAMT"))).compareTo(new BigDecimal("0")) > 0) {
                subgrckfs.add("1");
            } else {
                subgrckfs.add("0");
            }
            subgrckfs.add(getDXEBZ(src.get("TDCYCD"), src.get("TRANAMT")));
            subgrckfs.add(group_id);
            grckfs.add(subgrckfs);


            //}
        }
        result.add(grckjc);
        result.add(grckye);
        result.add(grckfs);
        return result;
    }
}
