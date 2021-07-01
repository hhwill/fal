package com.gingkoo.imas.hsbc.service;

public class EtlConst {

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

}
