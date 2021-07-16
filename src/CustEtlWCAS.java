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

import com.gingkoo.imas.core.batch.ImasBatchBasicValidateService;
import com.gingkoo.root.facility.spring.tx.TransactionHelper;

import static com.gingkoo.imas.hsbc.service.EtlConst.*;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlWCAS {

    private final Logger logger = LoggerFactory.getLogger(CustEtlWCAS.class);

    private final EtlInsertService insertService;

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    private ImasBatchBasicValidateService imasBatchBasicValidateService;

    public CustEtlWCAS(EtlInsertService insertService,TransactionHelper transactionTemplate,
                       ImasBatchBasicValidateService imasBatchBasicValidateService,
                       DataSource dataSource) {
        this.insertService = insertService;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.imasBatchBasicValidateService = imasBatchBasicValidateService;
        this.transactionTemplate = transactionTemplate;
    }

    private List<String> addWCAS_DGKHXX(String now, Map<String, Object> src) {
        List<String> result = new ArrayList<String>();
        result.add(now);
        result.add(formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS")));
        String nbjgh = formatNBJGH(src.get("ZGDCB"));
        result.add(nbjgh);
        String ZGCUCL = getString(src.get("ZGCUCL"));
        String ZGC2CN = getString(src.get("ZGC2CN"));
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

        //金融机构类型代码
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
                String XUSLTO = getString(src.get("XUSLTO"));
                if (XUSLTO.equals("5")) {
                    qygm = "CS01";
                } else if (XUSLTO.equals("2") || XUSLTO.equals("4") || XUSLTO.equals("3")) {
                    qygm = "CS02";
                } else {
                    String XUEMPE = getString(src.get("XUEMPE"));
                    if (XUEMPE.equals("L") || (XUEMPE.equals("O"))) {
                        qygm = "CS04";
                    } else if (XUEMPE.trim().length()> 1 && !XUEMPE.equals("0")) {
                        qygm = "CS03";
                    } else {
                        String LMSI = getString(src.get("S_LMSI"));
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
                    String ZGGHCL = getString(src.get("ZGGHCL"));
                    String XUCTHQ = getString(src.get("XUCTHQ"));
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
        //区划代码
        String xzqhdm = getMap("XDQDM", nbjgh);
        String ZUCSSN = getString(src.get("ZUCSSN"));
        if (ZUCSSN.contains("PARENT") || ZUCSSN.startsWith("P")) {
        } else {
            String ZUIDTY = getString(src.get("ZUIDTY"));
            if (ZUIDTY.equals("Z")) {
                String ZUIDNO = getString(src.get("ZUIDNO"));
                if (ZUIDNO.trim().length() == 18) {
                    String dqdm = ZUIDNO.substring(2,8);
                    if (getMap("DQQHDM", dqdm).equals("")) {
                        dqdm = dqdm.substring(0,4) + "00";
                        if (getMap("DQQHDM", dqdm).equals("")) {
                            dqdm = dqdm.substring(0,2) + "0000";
                            if (getMap("DQQHDM", dqdm).equals("")) {

                            } else {
                                xzqhdm = dqdm;
                            }
                        } else {
                            xzqhdm = dqdm;
                        }
                    } else {
                        xzqhdm = dqdm;
                    }
                }
            }
        }
        result.add(xzqhdm);
        //注册地址
        String ZBADID = getString(src.get("ZBADID"));
        String newAddress = getString(src.get("ADDRESS")).replace("（注册地址）","")
                .replace("（营业地址）","").replace("(REGISTERED ADDRESS)","")
                .replace("（REGISTERED ADDRESS）","").replace("(REGISTRATION ADDRESS)","")
                .replace("VAT USE ONLY","").replace(" ","").trim();
        if (ZBADID.equals("P9")) {
            result.add(newAddress);
        } else {
            result.add("");
        }
        result.add("");
        result.add("");
        result.add(getMap("X46", src.get("ZGINDY")));
        result.add("");
        return result;
    }

    public boolean checkDGKHXX(List<List<String>> base, Map<String, Object> src) {
        boolean find = false;
        String khh = formatKHH(src.get("ZGDCB")+"-"+src.get("ZGDCS"));
        for (int i = 0; i < base.size(); i++) {
            if (base.get(i).get(1).equals(khh)) {
                find = true;
                String ZUCSSN = getString(src.get("ZUCSSN"));
                if (ZUCSSN.contains("PARENT") || ZUCSSN.startsWith("P")) {
                } else {
                    String ZUIDTY = getString(src.get("ZUIDTY"));
                    if (ZUIDTY.equals("Z")) {
                        String ZUIDNO = getString(src.get("ZUIDNO"));
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
                if (base.get(i).get(9).trim().equals("")) {
                    String ZBADID = getString(src.get("ZBADID"));
                    String newAddress = getString(src.get("ADDRESS")).replace("（注册地址）", "")
                            .replace("（营业地址）", "").replace("(REGISTERED ADDRESS)", "")
                            .replace("（REGISTERED ADDRESS）", "").replace("(REGISTRATION ADDRESS)", "")
                            .replace("VAT USE ONLY", "").replace(" ", "").trim();
                    if (ZBADID.equals("P9")) {
                        base.get(i).set(9, newAddress);
                    }
                }
                break;
            }
        }
        return find;
    }





    public void processWCAS_DGHKXX(String now, List<Map<String, Object>> lstNow,
                                   List<Map<String, Object>> lstPrevious, String group_id) throws Exception {
        String day = now.substring(6,8);
        jdbcTemplate.update("delete from imas_pm_" + day + "_DGKHXX where sjrq = '"+now+"'");
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            String ZGC2CN = getString(record.get("ZGC2CN"));
            if (ZGC2CN.contains("DUMMY") || ZGC2CN.contains("INTERNAL TEST") || ZGC2CN.contains("过渡")) {
                continue;
            }
            if (!checkDGKHXX(base, record)) {
                base.add(addWCAS_DGKHXX(now, record));
            }
        }
        for (List<String> src : base) {
            if (src.get(9).equals("")) {
                src.set(9, getMap("DGHKXX_ADDRESS", src.get(2)));
            }
        }
        insertService.insertData(SQL_DGKHXX, group_id, group_id, base);
    }

    public void addDWCK_CORPDDAC(String now, Map<String, Object> src, List<List<String>> dwckjc,
                                 List<List<String>> dwckye) {
        List<List<String>> ckxx = getCKXH(src);
        for (int i = 0; i < ckxx.size(); i++) {
            List<String> subckxx = ckxx.get(i);
            List<String> subdwckjc = new ArrayList<String>();
            List<String> subdwckye = new ArrayList<String>();
            subdwckjc.add(now);
            subdwckjc.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
            subdwckjc.add(subckxx.get(2));
            subdwckjc.add(formatNBJGH(src.get("DFDCB")));
            subdwckjc.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
            //存款产品类别
            String CKCPLB = subckxx.get(3);
            Object DFAPTY = src.get("DFAPTY");
            Object DGCIRT = src.get("DGCIRT");
            if (DFAPTY.equals("CDP")) {
                if (DGCIRT.equals("C24")) {
                    CKCPLB = "D031";
                } else {
                    CKCPLB = "D032";
                }
            }
            if (ckxx.size() > 1) {
                if (i == 0) {
                    CKCPLB = "D051";
                } else {
                    CKCPLB = "D052";
                }
            }
            subdwckjc.add(CKCPLB);
            subdwckjc.add("");
            subdwckjc.add(getString(src.get("DFDTAO")));
            //到期日期
            String DQRQ = "";
            if (DFAPTY.equals("CDP") || DFAPTY.equals("S12"))  {
                if (DGCIRT.equals("C24")) {
                    DQRQ = "19990101";
                } else {
                    DQRQ = "19990107";
                }
            }
            subdwckjc.add(DQRQ);
            //实际终止日期
            String ZIDTAS = getMap("WCAS_CLOSEDAC", src.get("DFACB")+"_"+src.get("DFACS")+"_"+src.get("DFACX"));
            subdwckjc.add(ZIDTAS);
            subdwckjc.add("01");
            String key = src.get("DGCIRT") + "__" + src.get("DFCYCD");
            String value = getMap("WCAS_RATETYPE_DD", key);
            String[] rateType = new String[4];
            rateType[0] = "";
            rateType[1] = "";
            rateType[2] ="";
            rateType[3] = "";
            if (!value.equals("")) {
                try {
                    String[] ss = value.split("\\|");
                    if (ss.length > 0) {
                        rateType[0] = ss[0];
                    }
                    if (ss.length > 1) {
                        rateType[1] = ss[1];
                    }
                    if (ss.length > 2) {
                        rateType[2] = ss[2];
                    }
                    if (ss.length > 3) {
                        rateType[3] = ss[3];
                    }
                } catch (Exception ex) {

                }
            }
            subdwckjc.add(rateType[0]);
            subdwckjc.add(rateType[1]);
            subdwckjc.add(subckxx.get(0));
            //基准利率
            if (ckxx.size() > 1) {
                if (getString(src.get("DFCYCD")).equals("CNY")) {
                    if (i == 0) {
                        subdwckjc.add("0.35");
                    } else {
                        subdwckjc.add("1.15");
                    }
                } else {
                    subdwckjc.add("");
                }
            } else {
                subdwckjc.add(rateType[2]);
            }
            subdwckjc.add(rateType[3]);
            if (subckxx.get(3).equals("D08")) {
                subdwckjc.add(subckxx.get(0));
            } else {
                subdwckjc.add("");
            }
            subdwckjc.add("");
            subdwckjc.add("01");
            subdwckjc.add("N");
            subdwckjc.add(getDXEBZ(src.get("DFCYCD"), subckxx.get(1)));
            dwckjc.add(subdwckjc);
            subdwckye.add(now);
            subdwckye.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
            subdwckye.add(subckxx.get(2));
            subdwckye.add(formatNBJGH(src.get("DFDCB")));
            subdwckye.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
            subdwckye.add(getString(src.get("DFCYCD")));
            if (ZIDTAS.equals("")) {
                if (subckxx.get(1).startsWith("-")) {
                    subdwckye.add("0");
                } else {
                    if (subckxx.get(1).startsWith("-")) {
                        subdwckye.add("0");
                    } else {
                        subdwckye.add(subckxx.get(1));
                    }
                }
            } else {
                subdwckye.add("0");
            }
            dwckye.add(subdwckye);
        }
    }

    public void addTYCK_CORPDDAC(String now, Map<String, Object> src, List<List<String>> tyckjc,
                                 List<List<String>> tyckye) {
        List<List<String>> ckxx = getCKXH(src);
        List<String> subtyckjc = new ArrayList<String>();
        List<String> subtyckye = new ArrayList<String>();
        subtyckjc.add(now);
        subtyckjc.add(formatNBJGH(src.get("DFDCB")));
        subtyckjc.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        String ZGCUCL = getString(src.get("ZGCUCL"));
        String JRJGLXDM = getMap("X42", ZGCUCL).trim();
        if (JRJGLXDM.length() > 3) {
            JRJGLXDM = JRJGLXDM.substring(0,3);
        }
        if (JRJGLXDM.equals("0")) {
            JRJGLXDM = "";
        }
        subtyckjc.add(JRJGLXDM);
        subtyckjc.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
        subtyckjc.add("A011");
        subtyckjc.add(getString(src.get("DFDTAO")));
        subtyckjc.add("");
        subtyckjc.add("01");
        String key = src.get("DGCIRT") + "__" + src.get("DFCYCD");
        String value = getMap("WCAS_RATETYPE_DD", key);
        String[] rateType = new String[4];
        rateType[0] = "";
        rateType[1] = "";
        rateType[2] ="";
        rateType[3] = "";
        if (!value.equals("")) {
            try {
                String[] ss = value.split("\\|");
                if (ss.length > 0) {
                    rateType[0] = ss[0];
                }
                if (ss.length > 1) {
                    rateType[1] = ss[1];
                }
                if (ss.length > 2) {
                    rateType[2] = ss[2];
                }
                if (ss.length > 3) {
                    rateType[3] = ss[3];
                }
            } catch (Exception ex) {

            }
        }
        subtyckjc.add(rateType[0]);
        subtyckjc.add(rateType[1]);
        subtyckjc.add(ckxx.get(0).get(0));
        subtyckjc.add(rateType[2]);
        subtyckjc.add(rateType[3]);

        subtyckye.add(now);
        subtyckye.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
        subtyckye.add(formatNBJGH(src.get("DFDCB")));
        subtyckye.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
        subtyckye.add(getString(src.get("DFCYCD")));
        String LEDGER = getString(src.get("LEDGER"));
        if (LEDGER.startsWith("-")) {
            LEDGER = "0";
        }
        subtyckye.add(LEDGER);
        tyckjc.add(subtyckjc);
        tyckye.add(subtyckye);
    }

    public void addDWCK_CORPTDAC3(String now, Map<String, Object> src, List<List<String>> dwckjc,
                                 List<List<String>> dwckye) {
        List<List<String>> ckxx = getCKXH(src);
        List<String> subdwckjc = new ArrayList<String>();
        List<String> subdwckye = new ArrayList<String>();
        subdwckjc.add(now);
        String CKZHBM = formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX"));
        subdwckjc.add(CKZHBM);
        subdwckjc.add("01");
        subdwckjc.add(formatNBJGH(src.get("TDDCB")));
        subdwckjc.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        subdwckjc.add(getMap("WCAS_ProductType", src.get("TDAPTY")));
        subdwckjc.add("");
        subdwckjc.add(getString(src.get("TDSTDT")));
        //到期日期
        String TDAPTY = getString(src.get("TDAPTY"));
        if (TDAPTY.equals("CDT")) {
            subdwckjc.add("19990107");
        } else {
            subdwckjc.add(getString(src.get("TDDUDT")));
        }

        String ZIDTAS = getMap("WCAS_CLOSEDAC", src.get("TDACB")+"_"+src.get("TDACS")+"_"+src.get("TDACX"));
        subdwckjc.add(ZIDTAS);
        String TDTERM = getString(src.get("TDTERM"));
        if (!TDTERM.equals("0000")) {
            subdwckjc.add(getMap("WCAS_TERMCODE_FIX",TDTERM));
        } else {
            subdwckjc.add(checkWcasTendor(String.valueOf(differentDaysByMillisecond(getString(src.get("TDDUDT")),
                    getString(src.get("TDSTDT")))),
                    map.get("WCAS_TERMCODE")));
        }
        String TDCRTY = getString(src.get("TDCRTY"));
        String value = "";
        if (CKZHBM.equals("CNHSBC270000029204")) {
            logger.info(">>><<<");
            logger.info(src.toString());
            logger.info("TDAPTY:["+TDAPTY+"]");
        }
        if (TDAPTY.equals("D11") || TDAPTY.equals("D51") || TDAPTY.equals("D52") ||
                TDAPTY.equals("D54") || TDAPTY.equals("D55") ) {
            if (CKZHBM.equals("CNHSBC270000029204"))
                logger.info(">>><<<SD");
            value = getMap("WCAS_RATETYPE_SD", TDAPTY) ;
        } else {
            if (CKZHBM.equals("CNHSBC270000029204"))
                logger.info(">>><<<TD");
            value = getMap("WCAS_RATETYPE_TD", TDCRTY) ;
        }
        if (CKZHBM.equals("CNHSBC270000029204"))
            logger.info(">>><<<value:"+value);
        String[] rateType = new String[4];
        rateType[0] = "";
        rateType[1] = "";
        rateType[2] ="";
        rateType[3] = "";
        if (!value.equals("")) {
            try {
                String[] ss = value.split("\\|");
                if (ss.length > 0) {
                    rateType[0] = ss[0];
                }
                if (ss.length > 1) {
                    rateType[1] = ss[1];
                }
                if (ss.length > 2) {
                    rateType[2] = ss[2];
                }
                if (ss.length > 3) {
                    rateType[3] = ss[3];
                }
            } catch (Exception ex) {

            }
        }
        if (CKZHBM.equals("CNHSBC270000029204"))
            logger.info(">>><<<ratetype:"+rateType);
        subdwckjc.add(rateType[0]);
        subdwckjc.add(rateType[1]);
        subdwckjc.add(ckxx.get(0).get(0));
        subdwckjc.add(rateType[2]);
        subdwckjc.add(rateType[3]);
        if (ckxx.get(0).get(3).equals("D08")) {
            subdwckjc.add(ckxx.get(0).get(0));
        } else {
            subdwckjc.add("");
        }
        subdwckjc.add("");
        subdwckjc.add("01");
        subdwckjc.add("N");
        subdwckjc.add(getDXEBZ(src.get("TDCYCD"), ckxx.get(0).get(1)));
        dwckjc.add(subdwckjc);
        subdwckye.add(now);
        subdwckye.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
        subdwckye.add(ckxx.get(0).get(2));
        subdwckye.add(formatNBJGH(src.get("TDDCB")));
        subdwckye.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        subdwckye.add(getString(src.get("TDCYCD")));
        String LEDGER = getString(src.get("LEDGER"));
        if (LEDGER.startsWith("-")) {
            LEDGER = "0";
        }
        subdwckye.add(LEDGER);
        dwckye.add(subdwckye);
    }

    public void addTYCK_CORPTDAC3(String now, Map<String, Object> src, List<List<String>> tyckjc,
                                 List<List<String>> tyckye) {
        List<List<String>> ckxx = getCKXH(src);
        List<String> subtyckjc = new ArrayList<String>();
        List<String> subtyckye = new ArrayList<String>();
        subtyckjc.add(now);
        subtyckjc.add(formatNBJGH(src.get("TDDCB")));
        subtyckjc.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        String ZGCUCL = getString(src.get("ZGCUCL"));
        String JRJGLXDM = getMap("X42", ZGCUCL).trim();
        if (JRJGLXDM.length() > 3) {
            JRJGLXDM = JRJGLXDM.substring(0,3);
        }
        if (JRJGLXDM.equals("0")) {
            JRJGLXDM = "";
        }
        subtyckjc.add(JRJGLXDM);
        subtyckjc.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
        subtyckjc.add("A012");
        subtyckjc.add(getString(src.get("TDSTDT")));
        String TDAPTY = getString(src.get("TDAPTY"));
        if (TDAPTY.equals("CDT")) {
            subtyckjc.add("19990107");
        } else {
            subtyckjc.add(getString(src.get("TDDUDT")));
        }
        String TDTERM = getString(src.get("TDTERM"));
        if (!TDTERM.equals("0000")) {
            subtyckjc.add(getMap("WCAS_TERMCODE_FIX",TDTERM));
        } else {
            subtyckjc.add(checkWcasTendor(String.valueOf(differentDaysByMillisecond(getString(src.get("TDDUDT")),
                    getString(src.get("TDSTDT")))),
                    map.get("WCAS_TERMCODE")));
        }
        String TDCRTY = getString(src.get("TDCRTY"));
        String value = "";
        if (TDAPTY.equals("D11") || TDAPTY.equals("D51") || TDAPTY.equals("D52") ||
                TDAPTY.equals("D54") || TDAPTY.equals("D55") ) {
            value = getMap("WCAS_RATETYPE_SD", TDAPTY) ;
        } else {
            value = getMap("WCAS_RATETYPE_TD", TDCRTY) ;
        }
        String[] rateType = new String[4];
        rateType[0] = "";
        rateType[1] = "";
        rateType[2] ="";
        rateType[3] = "";
        if (!value.equals("")) {
            try {
                String[] ss = value.split("\\|");
                if (ss.length > 0) {
                    rateType[0] = ss[0];
                }
                if (ss.length > 1) {
                    rateType[1] = ss[1];
                }
                if (ss.length > 2) {
                    rateType[2] = ss[2];
                }
                if (ss.length > 3) {
                    rateType[3] = ss[3];
                }
            } catch (Exception ex) {

            }
        }
        subtyckjc.add(rateType[0]);
        subtyckjc.add(rateType[1]);
        subtyckjc.add(ckxx.get(0).get(0));
        subtyckjc.add(rateType[2]);
        subtyckjc.add(rateType[3]);

        subtyckye.add(now);
        subtyckye.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
        subtyckye.add(formatNBJGH(src.get("TDDCB")));
        subtyckye.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        subtyckye.add(getString(src.get("TDCYCD")));
        String LEDGER = getString(src.get("LEDGER"));
        if (LEDGER.startsWith("-")) {
            LEDGER = "0";
        }
        subtyckye.add(LEDGER);
        tyckjc.add(subtyckjc);
        tyckye.add(subtyckye);
    }

    private void addDDDWCKFS(String now, Map<String, Object> src, List<List<String>> dwckfs) {
        List<List<String>> ckxx = getCKXH(src);
        List<List<String>> jyls = getWCASJYLS(now, src);
        for (List<String> subjyls : jyls){
            List<String> result = new ArrayList<String>();
            result.add(now);
            String CKZHBH = formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX"));
            result.add(CKZHBH);
            result.add("01");
            result.add(formatNBJGH(src.get("DFDCB")));
            result.add(formatKHH(src.get("DFDCB") + "-" + src.get("DFDCS")));
            //交易流水号
            result.add(now+CKZHBH.substring(6)+getString(src.get("FSTYPE")));
            result.add(now);
            result.add(ckxx.get(0).get(0));
            String key = src.get("DGCIRT") + "__" + src.get("DFCYCD");
            String value = getMap("WCAS_RATETYPE_DD", key);
            String[] rateType = new String[4];
            rateType[0] = "";
            rateType[1] = "";
            rateType[2] ="";
            rateType[3] = "";
            if (!value.equals("")) {
                try {
                    String[] ss = value.split("\\|");
                    if (ss.length > 0) {
                        rateType[0] = ss[0];
                    }
                    if (ss.length > 1) {
                        rateType[1] = ss[1];
                    }
                    if (ss.length > 2) {
                        rateType[2] = ss[2];
                    }
                    if (ss.length > 3) {
                        rateType[3] = ss[3];
                    }
                } catch (Exception ex) {

                }
            }
            result.add(rateType[2]);
            result.add(getString(src.get("DFCYCD")));
            result.add(getString(src.get("LEDGER")));
            result.add("03");
            result.add(getString(src.get("JYFX")));
            result.add(getDXEBZ(src.get("DFCYCD"), src.get("LEDGER")));
            dwckfs.add(result);
        }
    }

    private void addDDTYCKFS(String now, Map<String, Object> src, List<List<String>> tyckfs) {
        List<List<String>> ckxx = getCKXH(src);
        List<List<String>> jyls = getWCASJYLS(now, src);
        for (List<String> subjyls : jyls) {
            List<String> result = new ArrayList<String>();
            result.add(now);
            String CKZHBH = formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX"));
            result.add(CKZHBH);
            result.add(formatNBJGH(src.get("DFDCB")));
            result.add(formatKHH(src.get("DFDCB") + "-" + src.get("DFDCS")));
            result.add(now+CKZHBH.substring(6)+getString(src.get("FSTYPE")));
            result.add(now);
            result.add(getString(src.get("DFCYCD")));

            String key = src.get("DGCIRT") + "__" + src.get("DFCYCD");
            String value = getMap("WCAS_RATETYPE_DD", key);
            String[] rateType = new String[4];
            rateType[0] = "";
            rateType[1] = "";
            rateType[2] ="";
            rateType[3] = "";
            if (!value.equals("")) {
                try {
                    String[] ss = value.split("\\|");
                    if (ss.length > 0) {
                        rateType[0] = ss[0];
                    }
                    if (ss.length > 1) {
                        rateType[1] = ss[1];
                    }
                    if (ss.length > 2) {
                        rateType[2] = ss[2];
                    }
                    if (ss.length > 3) {
                        rateType[3] = ss[3];
                    }
                } catch (Exception ex) {

                }
            }
            result.add(ckxx.get(0).get(0));
            result.add(rateType[2]);

            result.add(getString(src.get("LEDGER")));
            result.add(getString(src.get("JYFX")));
            tyckfs.add(result);
        }
    }

    private void addWCASDWCKFS(String now, Map<String, Object> src, List<List<String>> dwckfs) {
        List<List<String>> ckxx = getCKXH(src);
        List<List<String>> jyls = getWCASJYLS(now, src);
        for (List<String> subjyls : jyls){
            List<String> result = new ArrayList<String>();
            result.add(now);
            result.add(formatCKZHBH(src.get("TDACB"), src.get("TDACS"), src.get("TDACX")));
            result.add(ckxx.get(0).get(2));
            result.add(formatNBJGH(src.get("TDDCB")));
            result.add(formatKHH(src.get("TDDCB") + "-" + src.get("TDDCS")));
            result.add(subjyls.get(0));
            result.add(subjyls.get(1));
            result.add(ckxx.get(0).get(0));
            String TDAPTY = getString(src.get("TDAPTY"));
            String TDCRTY = getString(src.get("TDCRTY"));
            String value = "";
            if (TDAPTY.equals("D11") || TDAPTY.equals("D51") || TDAPTY.equals("D52") ||
                    TDAPTY.equals("D54") || TDAPTY.equals("D55") ) {
                value = getMap("WCAS_RATETYPE_SD", TDAPTY) ;
            } else {
                value = getMap("WCAS_RATETYPE_TD", TDCRTY) ;
            }
            String[] rateType = new String[4];
            rateType[0] = "";
            rateType[1] = "";
            rateType[2] = "";
            rateType[3] = "";
            if (!value.equals("")) {
                try {
                    String[] ss = value.split("\\|");
                    if (ss.length > 0) {
                        rateType[0] = ss[0];
                    }
                    if (ss.length > 1) {
                        rateType[1] = ss[1];
                    }
                    if (ss.length > 2) {
                        rateType[2] = ss[2];
                    }
                    if (ss.length > 3) {
                        rateType[3] = ss[3];
                    }
                } catch (Exception ex) {

                }
            }
            result.add(rateType[2]);
            result.add(getString(src.get("TDCYCD")));
            result.add(subjyls.get(2));
            result.add("03");
            result.add(subjyls.get(3));
            result.add(getDXEBZ(src.get("TDCYCD"), subjyls.get(2)));
            dwckfs.add(result);
        }
    }

    private void addWCASTYCKFS(String now, Map<String, Object> src, List<List<String>> tyckfs) {
        List<List<String>> ckxx = getCKXH(src);
        List<List<String>> jyls = getWCASJYLS(now, src);
        for (List<String> subjyls : jyls) {
            List<String> result = new ArrayList<String>();
            result.add(now);
            result.add(formatCKZHBH(src.get("TDACB"), src.get("TDACS"), src.get("TDACX")));
            result.add(formatNBJGH(src.get("TDDCB")));
            result.add(formatKHH(src.get("TDDCB") + "-" + src.get("TDDCS")));
            result.add(subjyls.get(0));
            result.add(subjyls.get(1));
            result.add(getString(src.get("TDCYCD")));
            String TDAPTY = getString(src.get("TDAPTY"));
            String TDCRTY = getString(src.get("TDCRTY"));
            String value = "";
            if (TDAPTY.equals("D11") || TDAPTY.equals("D51") || TDAPTY.equals("D52") ||
                    TDAPTY.equals("D54") || TDAPTY.equals("D55") ) {
                value = getMap("WCAS_RATETYPE_SD", TDAPTY) ;
            } else {
                value = getMap("WCAS_RATETYPE_TD", TDCRTY) ;
            }
            String[] rateType = new String[4];
            rateType[0] = "";
            rateType[1] = "";
            rateType[2] ="";
            rateType[3] = "";
            if (!value.equals("")) {
                try {
                    String[] ss = value.split("\\|");
                    if (ss.length > 0) {
                        rateType[0] = ss[0];
                    }
                    if (ss.length > 1) {
                        rateType[1] = ss[1];
                    }
                    if (ss.length > 2) {
                        rateType[2] = ss[2];
                    }
                    if (ss.length > 3) {
                        rateType[3] = ss[3];
                    }
                } catch (Exception ex) {

                }
            }
            result.add(ckxx.get(0).get(0));
            result.add(rateType[2]);

            result.add(subjyls.get(2));
            result.add(subjyls.get(3));
            tyckfs.add(result);
        }
    }

    public boolean process(String now, String group_id) throws Exception {
        logger.info(">>>开始WCAS" + now + " " + group_id);
        String day = now.substring(6,8);
        String sql = String.format("delete from imas_pm_"+day+"_dwckjc where sjrq = '%s' and group_id = '%s'", now,
                group_id);
        jdbcTemplate.update(sql);
        sql = String.format("delete from imas_pm_"+day+"_dwckye where sjrq = '%s' and group_id = '%s'", now, group_id);
        jdbcTemplate.update(sql);
        sql = String.format("delete from imas_pm_"+day+"_tyckjc where sjrq = '%s' and group_id = '%s'", now, group_id);
        jdbcTemplate.update(sql);
        sql = String.format("delete from imas_pm_"+day+"_tyckye where sjrq = '%s' and group_id = '%s'", now, group_id);
        jdbcTemplate.update(sql);
        sql = String.format("delete from imas_pm_"+day+"_dwckfs where sjrq = '%s' and group_id = '%s'", now, group_id);
        jdbcTemplate.update(sql);
        sql = String.format("delete from imas_pm_"+day+"_tyckfs where sjrq = '%s' and group_id = '%s'", now, group_id);
        jdbcTemplate.update(sql);
        sql = "select * from ODS_WCAS_CORPDDAC where data_date = '" + now + "' and group_id = '" + group_id + "'";
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        processCORPDDAC(now, lst, group_id);
        sql = "select * from ODS_WCAS_CORPTDAC3 where data_date = '" + now + "' and group_id = '" + group_id + "'";
        lst = jdbcTemplate.queryForList(sql);
        processCORPTDAC3(now, lst, group_id);

//        sql = String.format("update imas_pm_dwckjc set bdsyl = sjll where group_id='%s' and sjrq='%s' and ckcplb='D08'", now, group_id);
//        int result = jdbcTemplate.update(sql);
//        logger.info(">>>>D08 " + result + ",开始校验");
//        try {
//            List<String> lst2 = new ArrayList<String>();
//            lst2.add("DWCKJC");
//            lst2.add("DWCKYE");
//            lst2.add("TYCKJC");
//            lst2.add("TYCKTE");
//            lst2.add("DWCKFS");
//            lst2.add("TYCKFS");
//            imasBatchBasicValidateService.validate(now, lst2, "HSBC", group_id, null, true);
//        } catch (Exception ex) {
//            logger.error(">>>>校验出错", ex);
//        }
//        logger.info(">>>>结束校验");
        return true;
    }

    public void processCORPDDAC(String now, List<Map<String, Object>> lstNow,
                                String group_id) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            boolean need = false;
            String ZGC2CN = getString(record.get("ZGC2CN"));
            if (ZGC2CN.contains("DUMMY") || ZGC2CN.contains("INTERNAL TEST") || ZGC2CN.contains("过渡")) {
                continue;
            }
            String DFSTUS = getString(record.get("DFSTUS"));
            if (DFSTUS != null && !DFSTUS.equals("4") && !DFSTUS.equals("5") ) {
                need = true;
            }
            String ZIDTAS = getMap("WCAS_CLOSEDAC",
                    getString(record.get("DFACB"))+"_"+getString(record.get("DFACS"))+"_"+getString(record.get(
                    "DFACX")));
            if (!ZIDTAS.equals("")) {
                need = true;
            }
            if (need) {
                String ZGCUCL = getString(record.get("ZGCUCL"));
                String tybz = getMap("WCAS_TYBZ", ZGCUCL);
                if (tybz.equals("非同业")) {
                    addDWCK_CORPDDAC(now, record, dwckjc, dwckye);
                } else {
                    addTYCK_CORPDDAC(now, record, tyckjc, tyckye);
                }
            }
        }
        insertService.insertData(SQL_DWCKJC, group_id, group_id, dwckjc);
        insertService.insertData(SQL_DWCKYE, group_id, group_id, dwckye);
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
    }

    private boolean processfs(String now, String group_id) {
        String sql = "select max(data_date) from ods_wpb_corpddac where data_date < '" + now + "'";
        String previous = jdbcTemplate.queryForObject(sql, String.class);
        sql = String.format("select a.*, 'W' as FSTYPE, '2' as JYFX from ( select * from ods_wpb_corpddac where " +
                        "data_date"
                        + " = '%s') a inner join (select * from ods_wpb_corpddac where data_date = '%s') b "
                        + " on a.dfacb=b.dfacb and a.dfacs=b.dfacs and a.dfacx = b.dfacx and a.ledger=0 and b" +
                        ".ledger<>0" +
                        " and" +
                        " a.DFAPTY in ('CDP','S12')"
                        + " union "
                        + " select a.*, 'N' as FSTYPE, '1' as JYFX from (select * from ods_wpb_corpddac where data_date"
                        + " = '%s' and DFAPTY in ('CDP','S12')) a where not exists (select * " +
                        "from (select dfacb,dfacs,dfacx from ods_wpb_corpddac "
                        + " where data_date = '%s') b where a.dfacb=b.dfacb and a.dfacs=b.dfacs and a.dfacx = b.dfacx)",
                now, previous, now, previous);
        List<Map<String, Object>> lst = jdbcTemplate.queryForList(sql);
        List<List<String>> dwckfs = new ArrayList<List<String>>();
        List<List<String>> tyckfs = new ArrayList<List<String>>();
        for (Map<String, Object> src : lst) {

            String ZGCUCL = getString(src.get("ZGCUCL"));
            String tybz = getMap("WCAS_TYBZ", ZGCUCL);
            if (tybz.equals("非同业")) {
                addDDDWCKFS(now, src, dwckfs);
            } else {
                addDDTYCKFS(now, src, tyckfs);
            }
        }
        try {
            insertService.insertData(SQL_DWCKFS, group_id, group_id, dwckfs);
            insertService.insertData(SQL_TYCKFS, group_id, group_id, tyckfs);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return true;
    }

    public void processCORPTDAC3(String now, List<Map<String, Object>> lstNow,
                                 String group_id) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        List<List<String>> dwckfs = new ArrayList<List<String>>();
        List<List<String>> tyckfs = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            boolean need = false;
            String ZGC2CN = getString(record.get("ZGC2CN"));
            if (ZGC2CN.contains("DUMMY") || ZGC2CN.contains("INTERNAL TEST") || ZGC2CN.contains("过渡")) {
                continue;
            }
            String ZIDTAS = getMap("WCAS_CLOSEDAC",
                    getString(record.get("TDACB"))+"_"+getString(record.get("TDACS"))+"_"+getString(record.get(
                    "TDACX")));
            if (!ZIDTAS.equals("")) {
                need = true;
            }
            String DFSTUS = getString(record.get("TDSTUS"));
            String ZGCUCL = getString(record.get("ZGCUCL"));
            String tybz = getMap("WCAS_TYBZ", ZGCUCL);
            String THCPDT = getString(record.get("THCPDT"));
            if (DFSTUS.equals("1") || DFSTUS.equals("2") || DFSTUS.equals("3") || (DFSTUS.equals("4") && THCPDT.equals(now))) {
                need = true;
            }
            if (need) {
                if (tybz.equals("非同业")) {
                    addDWCK_CORPTDAC3(now, record, dwckjc, dwckye);
                } else {
                    addTYCK_CORPTDAC3(now, record, tyckjc, tyckye);
                }
            }
            if (tybz.equals("非同业")) {
                if (record.get("THCPDT").equals(now) || (record.get("TDSTDT").equals(now) && record.get("THCPDT").equals("0") && record.get("TDMTIN").equals("2") && record.get("TDSTUS").equals("1"))) {
                    addWCASDWCKFS(now, record, dwckfs);
                }
            } else {
                if (record.get("THCPDT").equals(now) || (record.get("TDSTDT").equals(now) && record.get("THCPDT").equals("0") && record.get("TDMTIN").equals("2") && record.get("TDSTUS").equals("1"))) {
                    addWCASTYCKFS(now, record, tyckfs);
                }
            }
        }
        insertService.insertData(SQL_DWCKJC, group_id, group_id, dwckjc);
        insertService.insertData(SQL_DWCKYE, group_id, group_id, dwckye);
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
        insertService.insertData(SQL_DWCKFS, group_id, group_id, dwckfs);
        insertService.insertData(SQL_TYCKFS, group_id, group_id, tyckfs);
    }

    private void execUpdSqlCommit(String sql) {
        transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
            jdbcTemplate.update(sql);
        });
    }

    public void test() {
        //jdbcTemplate.update("delete from imas_pm_31_dwckjc");
        execUpdSqlCommit("delete from imas_pm_31_dwckjc");
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
        List<Map<String, Object>> lst = jdbcTemplate.queryForList("select * from imas_pm_31_dwckjc");
        System.out.println(">>><<<");
        System.out.println(lst);
    }
}
