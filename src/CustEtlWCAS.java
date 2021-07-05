package com.gingkoo.imas.hsbc.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlConst.*;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Slf4j
@Component
public class CustEtlWCAS {

    private final EtlInsertService insertService;

    public CustEtlWCAS(EtlInsertService insertService) {
        this.insertService = insertService;
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
        result.add(getMap("XDQDM", nbjgh));
        result.add(getString(src.get("ADDRESS")).replace("（注册地址）","").trim());
        result.add("0");
        result.add("0");
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
                String ZBADID = getString(src.get("ZBADID"));
                if (ZBADID.equals("09")) {
                    base.get(i).set(9, getString(src.get("ADDRESS")).replace("（注册地址）","").trim());
                }
                break;
            }
        }
        return find;
    }





    public void processWCAS_DGHKXX(String now, List<Map<String, Object>> lstNow,
                                   List<Map<String, Object>> lstPrevious, String group_id) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            if (!checkDGKHXX(base, record)) {
                base.add(addWCAS_DGKHXX(now, record));
            }
        }
        insertService.insertData(SQL_DGKHXX, group_id, group_id, base);
    }

    public void addDWCK_CORPDDAC(String now, Map<String, Object> src, List<List<String>> dwckjc,
                                 List<List<String>> dwckye) {
        List<List<String>> ckxx = getCKXH(src);
        for (List<String> subckxx : ckxx) {
            List<String> subdwckjc = new ArrayList<String>();
            List<String> subdwckye = new ArrayList<String>();
            subdwckjc.add(now);
            subdwckjc.add(formatCKZHBH(src.get("DFACB"),src.get("DFACS"),src.get("DFACX")));
            subdwckjc.add(subckxx.get(2));
            subdwckjc.add(formatNBJGH(src.get("DFDCB")));
            subdwckjc.add(formatKHH(src.get("DFDCB")+"-"+src.get("DFDCS")));
            subdwckjc.add(subckxx.get(3));
            subdwckjc.add("");
            subdwckjc.add(getString(src.get("DFDTAO")));
            subdwckjc.add("");
            String ZIDTAS = getMap("WCAS_CLOSEDAC", src.get("DFACB")+"_"+src.get("DFACS")+"_"+src.get("DFACX"));
            subdwckjc.add(ZIDTAS);
            subdwckjc.add("01");
            String key = src.get("DGCIRT") + "__" + src.get("DFCYCD");
            String value = getMap("RATETYPE", key);
            String[] rateType = new String[4];
            rateType[0] = "TR01";
            rateType[1] = "RF01";
            rateType[2] ="5.2";
            rateType[3] = "01";
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
            subdwckjc.add(rateType[2]);
            subdwckjc.add(rateType[3]);
            if (subckxx.get(3).equals("D08")) {
                subdwckjc.add(subckxx.get(0));
            } else {
                subdwckjc.add("");
            }
            subdwckjc.add("");
            subdwckjc.add("01");
            subdwckjc.add("N");
            String ccy = getString(src.get("DFCYCD"));
            if (ccy.equals("CNY")) {
                subdwckjc.add("");
            } else {
                //usd >=300W then A else B
                if (ccy.equals("EUR") || ccy.equals("HKD") || ccy.equals("JPY")) {
                    String currate = getMap("RATE", ccy +"/USD");
                    if (!currate.equals("")) {
                        BigDecimal x = new BigDecimal(subckxx.get(1)).multiply(new BigDecimal(currate));
                        if (x.compareTo(new BigDecimal("3000000")) > -1) {
                            subdwckjc.add("A");
                        } else {
                            subdwckjc.add("B");
                        }
                    } else {
                        subdwckjc.add("");
                    }
                } else {
                    subdwckjc.add("");
                }
            }
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
                    subdwckye.add(subckxx.get(1));
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
        String value = getMap("RATETYPE", key);
        String[] rateType = new String[4];
        rateType[0] = "TR01";
        rateType[1] = "RF01";
        rateType[2] ="5.2";
        rateType[3] = "01";
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
        subtyckye.add(getString(src.get("LEDGER")));
        tyckjc.add(subtyckjc);
        tyckye.add(subtyckye);
    }

    public void addDWCK_CORPTDAC3(String now, Map<String, Object> src, List<List<String>> dwckjc,
                                 List<List<String>> dwckye) {
        List<List<String>> ckxx = getCKXH(src);
        List<String> subdwckjc = new ArrayList<String>();
        List<String> subdwckye = new ArrayList<String>();
        subdwckjc.add(now);
        subdwckjc.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
        subdwckjc.add("01");
        subdwckjc.add(formatNBJGH(src.get("TDDCB")));
        subdwckjc.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        subdwckjc.add(getMap("WCAS_ProductType", src.get("TDAPTY")));
        subdwckjc.add("");
        subdwckjc.add(getString(src.get("TDSTDT")));
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
        String key = src.get("TDCRTY") + "_"+src.get("TDTERM")+"_" + src.get("TDCYCD");
        String value = getMap("RATETYPE", key);
        String[] rateType = new String[4];
        rateType[0] = "TR01";
        rateType[1] = "RF01";
        rateType[2] ="5.2";
        rateType[3] = "01";
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
        String ccy = getString(src.get("TDCYCD"));
        if (ccy.equals("CNY")) {
            subdwckjc.add("");
        } else {
            //usd >=300W then A else B
            if (ccy.equals("EUR") || ccy.equals("HKD") || ccy.equals("JPY")) {
                String currate = getMap("RATE", ccy +"/USD");
                if (!currate.equals("")) {
                    BigDecimal x = new BigDecimal(ckxx.get(0).get(1)).multiply(new BigDecimal(currate));
                    if (x.compareTo(new BigDecimal("3000000")) > -1) {
                        subdwckjc.add("A");
                    } else {
                        subdwckjc.add("B");
                    }
                } else {
                    subdwckjc.add("");
                }
            } else {
                subdwckjc.add("");
            }
        }
        dwckjc.add(subdwckjc);
        subdwckye.add(now);
        subdwckye.add(formatCKZHBH(src.get("TDACB"),src.get("TDACS"),src.get("TDACX")));
        subdwckye.add(ckxx.get(0).get(2));
        subdwckye.add(formatNBJGH(src.get("TDDCB")));
        subdwckye.add(formatKHH(src.get("TDDCB")+"-"+src.get("TDDCS")));
        subdwckye.add(getString(src.get("TDCYCD")));
        subdwckye.add(getString(src.get("LEDGER")));
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
        String key = src.get("TDCRTY") + "_"+src.get("TDTERM")+"_" + src.get("TDCYCD");
        String value = getMap("RATETYPE", key);
        String[] rateType = new String[4];
        rateType[0] = "TR01";
        rateType[1] = "RF01";
        rateType[2] ="5.2";
        rateType[3] = "01";
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
        subtyckye.add(getString(src.get("LEDGER")));
        tyckjc.add(subtyckjc);
        tyckye.add(subtyckye);
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
            String key = src.get("TDCRTY") + "_" + src.get("TDTERM") + "_" + src.get("TDCYCD");
            String value = getMap("RATETYPE", key);
            String[] rateType = new String[4];
            rateType[0] = "TR01";
            rateType[1] = "RF01";
            rateType[2] = "5.2";
            rateType[3] = "01";
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
            String ccy = getString(src.get("TDCYCD"));
            if (ccy.equals("CNY")) {
                result.add("");
            } else {
                //usd >=300W then A else B
                if (ccy.equals("EUR") || ccy.equals("HKD") || ccy.equals("JPY")) {
                    String currate = getMap("RATE", ccy + "/USD");
                    if (!currate.equals("")) {
                        BigDecimal x = new BigDecimal(subjyls.get(2)).multiply(new BigDecimal(currate));
                        if (x.compareTo(new BigDecimal("3000000")) > -1) {
                            result.add("A");
                        } else {
                            result.add("B");
                        }
                    } else {
                        result.add("");
                    }
                } else {
                    result.add("");
                }
            }
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
            String key = src.get("TDCRTY") + "_"+src.get("TDTERM")+"_" + src.get("TDCYCD");
            String value = getMap("RATETYPE", key);
            String[] rateType = new String[4];
            rateType[0] = "TR01";
            rateType[1] = "RF01";
            rateType[2] ="5.2";
            rateType[3] = "01";
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

    public void processCORPDDAC(String now, List<Map<String, Object>> lstNow, List<Map<String, Object>> lstPrevious,
                                String group_id) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            String DFSTUS = getString(record.get("DFSTUS"));
            if (DFSTUS != null && !DFSTUS.equals("4") && !DFSTUS.equals("5") ) {
                String ZGCUCL = getString(record.get("ZGCUCL"));
                String tybz = getMap("WCAS_TYBZ", ZGCUCL);
                if (tybz.equals("非同业")) {
                    addDWCK_CORPDDAC(now, record, dwckjc, dwckye);
                } else {
                    addTYCK_CORPDDAC(now, record, tyckjc, tyckye);
                }
            }
        }
        log.info(">>>>>>>>dwckjc");
        log.info(dwckjc.toString());
        log.info(">>>>>>>>dwckye");
        log.info(dwckye.toString());
        insertService.insertData(SQL_DWCKJC, group_id, group_id, dwckjc);
        insertService.insertData(SQL_DWCKYE, group_id, group_id, dwckye);
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
    }

    public void processCORPTDAC3(String now, List<Map<String, Object>> lstNow, List<Map<String, Object>> lstPrevious,
                                 String group_id) throws Exception {
        List<List<String>> dwckjc = new ArrayList<List<String>>();
        List<List<String>> dwckye = new ArrayList<List<String>>();
        List<List<String>> tyckjc = new ArrayList<List<String>>();
        List<List<String>> tyckye = new ArrayList<List<String>>();
        List<List<String>> dwckfs = new ArrayList<List<String>>();
        List<List<String>> tyckfs = new ArrayList<List<String>>();
        for (Map<String, Object> record : lstNow) {
            String DFSTUS = getString(record.get("TDSTUS"));
            String ZGCUCL = getString(record.get("ZGCUCL"));
            String tybz = getMap("WCAS_TYBZ", ZGCUCL);
            if (DFSTUS != null && !DFSTUS.equals("4") && !DFSTUS.equals("5") ) {
                if (tybz.equals("非同业")) {
                    addDWCK_CORPTDAC3(now, record, dwckjc, dwckye);
                    if (record.get("THCPDT").equals(now) || (record.get("TDSTDT").equals(now) && record.get("THCPDT").equals("0") && record.get("TDMTIN").equals("2") && record.get("TDSTUS").equals("1"))) {
                        addWCASDWCKFS(now, record, dwckfs);
                    }
                } else {
                    addTYCK_CORPTDAC3(now, record, tyckjc, tyckye);
                    if (record.get("THCPDT").equals(now) || (record.get("TDSTDT").equals(now) && record.get("THCPDT").equals("0") && record.get("TDMTIN").equals("2") && record.get("TDSTUS").equals("1"))) {
                        addWCASTYCKFS(now, record, tyckfs);
                    }
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
