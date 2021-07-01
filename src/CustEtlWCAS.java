package com.gingkoo.imas.hsbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import static com.gingkoo.imas.hsbc.service.EtlConst.*;
import static com.gingkoo.imas.hsbc.service.EtlUtils.*;

@Component
public class CustEtlWCAS {

    private final EtlInsertService insertService;

    public CustEtlWCAS(EtlInsertService insertService) {
        this.insertService = insertService;
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

    public void processWCAS_DGHKXX(String now, List<Map<String, String>> lstNow,
                                   List<Map<String, String>> lstPrevious, String group_id) throws Exception {
        List<List<String>> base = new ArrayList<List<String>>();
        for (Map<String, String> record : lstNow) {
            if (!checkDGKHXX(base, record)) {
                base.add(addWCAS_DGKHXX(now, record));
            }
        }
        insertService.insertData(SQL_DGKHXX, group_id, group_id, base);
    }

    public void processCORPDDAC(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious, String group_id) throws Exception {
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
        insertService.insertData(SQL_DWCKJC, group_id, group_id, dwckjc);
        insertService.insertData(SQL_DWCKYE, group_id, group_id, dwckye);
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
    }

    public void processCORPTDAC3(String now, List<Map<String, String>> lstNow, List<Map<String, String>> lstPrevious, String group_id) throws Exception {
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
        insertService.insertData(SQL_DWCKJC, group_id, group_id, dwckjc);
        insertService.insertData(SQL_DWCKYE, group_id, group_id, dwckye);
        insertService.insertData(SQL_TYCKJC, group_id, group_id, tyckjc);
        insertService.insertData(SQL_TYCKYE, group_id, group_id, tyckye);
        insertService.insertData(SQL_DWCKFS, group_id, group_id, dwckfs);
        insertService.insertData(SQL_TYCKFS, group_id, group_id, tyckfs);
    }

}
