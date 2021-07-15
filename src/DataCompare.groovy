package scripts.imas.imp

import com.gingkoo.data.compute.api.bean.Result
import com.gingkoo.data.compute.impl.support.ComputeScript
import com.gingkoo.imas.core.config.ImasSysConfigService
import com.gingkoo.imas.core.dbsharding.ImasDbShardingService
import com.gingkoo.orm.entity.ImasBmRptCfg
import com.gingkoo.orm.repository.ImasBmRptCfgRepository
import com.gingkoo.rdms.base.data.g.entity.RdmsDsTableFieldCfg
import com.gingkoo.rdms.base.data.g.repository.RdmsDsTableFieldCfgRepository
import com.gingkoo.root.annotation.Nullable
import com.gingkoo.root.facility.database.DatabaseDialect
import org.apache.commons.lang3.StringUtils
import org.springframework.util.StopWatch

import java.time.LocalDate
import java.util.stream.Collectors

import static com.gingkoo.root.facility.database.sql.SqlHelper.cleanSpaces
import static com.gingkoo.root.facility.datetime.DateConstants.SIMPLE_DATE

class DataCompare implements ComputeScript {

    ImasBmRptCfgRepository rptCfgRepository
    RdmsDsTableFieldCfgRepository tableFieldCfgRepository
    ImasDbShardingService dbShardingService
    ImasSysConfigService sysConfigService
    DatabaseDialect databaseDialect

    DataCompare(ImasBmRptCfgRepository rptCfgRepository,
                RdmsDsTableFieldCfgRepository tableFieldCfgRepository,
                ImasDbShardingService dbShardingService,
                ImasSysConfigService sysConfigService,
                DatabaseDialect databaseDialect) {
        this.rptCfgRepository = rptCfgRepository
        this.tableFieldCfgRepository = tableFieldCfgRepository
        this.dbShardingService = dbShardingService
        this.sysConfigService = sysConfigService
        this.databaseDialect = databaseDialect
    }

    /**
     *
     * @param reportCode 报表编码
     * @param dataRptDate 报表日期
     * @param cond 可选参数，额外的条件，格式为sql片段
     */
    Result call(@Nullable String jobId, String dataRptDate, String cond, String reportCode) {
        log.info("jobId: ${jobId} dataRptDate:${dataRptDate} reportCode:${reportCode}")
        ImasBmRptCfg rptCfg = rptCfgRepository.getFirstByReportCode(reportCode)
        String collectMode = rptCfg.collectMode
        log.info("collectMode is ${collectMode}")

        List<RdmsDsTableFieldCfg> tableFieldCfgList = tableFieldCfgRepository.queryByDatasetId("IMAS_PM_" + reportCode)

        List<String> pks = tableFieldCfgList.stream()
                .filter(cfg -> cfg.keyType == "U")
                .map(RdmsDsTableFieldCfg::getFieldId)
                .collect(Collectors.toList())
        List<String> buses = tableFieldCfgList.stream()
                .filter(cfg -> cfg.isReport == "Y" && !pks.contains(cfg.fieldId))
                .map(RdmsDsTableFieldCfg::getFieldId)
                .collect(Collectors.toList())
        pks.remove("SJRQ")

        if (StringUtils.isBlank(cond)) {
            cond = " 1=1 "
        }

        String tableName = dbShardingService.shardingPmByReportCode(reportCode, dataRptDate)
        String preTableName = dbShardingService.shardingPmByReportCode(reportCode, dataRptDate, true)
        String preDataRptDate = SIMPLE_DATE.format(LocalDate.parse(dataRptDate, SIMPLE_DATE).minusDays(1));

        if (databaseDialect == DatabaseDialect.MYSQL) {
            return mysql(dataRptDate, preDataRptDate, tableName, preTableName, collectMode, pks, buses, cond)
        } else if (databaseDialect == DatabaseDialect.SQLSERVER) {
            return sqlserver(dataRptDate, preDataRptDate, tableName, preTableName, collectMode, pks, buses, cond)
        } else {
            return oracle(dataRptDate, preDataRptDate, tableName, preTableName, collectMode, pks, buses, cond)
        }
    }

    Result oracle(String dataRptDate, String preDataRptDate, String tableName, String preTableName, String collectMode, List<String> pks, List<String> buses, String cond) {
        StopWatch sw = new StopWatch("oracle data compare for $tableName $dataRptDate")
        String abPkEqCond = pks.stream().map(pk -> " a.${pk} = b.${pk} ").collect(Collectors.joining(" and "))
        String busDiffCond = buses.stream().map(pk -> " a.${pk} != b.${pk} or (a.${pk} is null and b.${pk} is not null) or (a.${pk} is not null and b.${pk} is null)").collect(Collectors.joining(" or "))
        String busEqCond = buses.stream().map(pk -> " (a.${pk} = b.${pk} or (a.${pk} is null and b.${pk} is null)) ").collect(Collectors.joining(" and "))

        log.info("collectMode is ${collectMode}")
        if (collectMode == "1") {
            // 变化量
            // 更新新增数据上报类型为 新增
            sw.start("mod collect update to A")
            def updateASql = cleanSpaces("""
                update ${tableName} a
                set a.DATA_RPT_FLAG = 'A'
                where a.SJRQ = '${dataRptDate}'
                  and a.NEXT_ACTION != '99'  
                  and ${cond.replace("alias.", "a.")} 
                  and not exists(select 1 from ${preTableName} b where b.SJRQ = '$preDataRptDate' and ${abPkEqCond})
            """.toString())
            log.debug("mod collect update to A sql[$updateASql]")
            sql.executeUpdate(updateASql)
            sw.stop()
            def whereSqlSegment = """
                where SJRQ = '${dataRptDate}' and NEXT_ACTION!='99' and ${cond.replace("alias.", "")} and exists(
                              select 1
                              from ${preTableName} b
                              where b.SJRQ = '$preDataRptDate'
                                and ${abPkEqCond}
                                and (_busCond_))
            """.toString()
            // 更新新增数据上报类型为 修改
            sw.start("mod collect update to M")
            def updateMSql = cleanSpaces("update ${tableName} a set a.DATA_RPT_FLAG = 'M' ${whereSqlSegment}".replace("_busCond_", busDiffCond).toString())
            log.debug("mod collect update to M sql[$updateMSql]")
            sql.executeUpdate(updateMSql)
            sw.stop()
            // 更新新增数据上报类型为 无变化
            sw.start("mod collect update to O")
            def updateOSql = cleanSpaces("""
                merge into ${tableName} a 
                using (select * from ${preTableName} where SJRQ = '$preDataRptDate') b
                on (${abPkEqCond} and (_busCond_))
                when matched then 
                update set a.DATA_RPT_FLAG = 'O', a.CHECK_FLAG = b.CHECK_FLAG where SJRQ = '${dataRptDate}' and NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} 
                """.replace("_busCond_", busEqCond).toString())
            log.debug("mod collect update to O sql[$updateOSql]")
            sql.executeUpdate(updateOSql)
            sw.stop()
        } else {
            sw.start("add collect update to A")
            def updateASql = """
                update ${tableName}
                set DATA_RPT_FLAG = 'A'
                where SJRQ = '${dataRptDate}'
                  and NEXT_ACTION != '99' 
                  and ${cond.replace("alias.", "")} 
            """.toString()
            sql.executeUpdate(cleanSpaces(updateASql))
            sw.stop()
        }
        log.info(sw.prettyPrint())
        return Result.OK
    }

    Result mysql(String dataRptDate, String preDataRptDate, String tableName, String preTableName, String collectMode, List<String> pks, List<String> buses, String cond) {
        StopWatch sw = new StopWatch("mysql data compare for $tableName $dataRptDate")
        String abPkEqCond = pks.stream().map(pk -> " a.${pk} = b.${pk} ").collect(Collectors.joining(" and "))
        String busDiffCond = buses.stream().map(pk -> " a.${pk} != b.${pk} or (a.${pk} is null and b.${pk} is not null) or (a.${pk} is not null and b.${pk} is null)").collect(Collectors.joining(" or "))
        String busEqCond = buses.stream().map(pk -> " (a.${pk} = b.${pk} or (a.${pk} is null and b.${pk} is null)) ").collect(Collectors.joining(" and "))

        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYCKJC")) {
            sql.execute("call SP_UPDATE_TYCKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYCKYE")) {
            sql.execute("call SP_UPDATE_TYCKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWDKJC")) {
            sql.execute("call SP_UPDATE_DWDKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWDKYE")) {
            sql.execute("call SP_UPDATE_DWDKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYJDJC")) {
            sql.execute("call SP_UPDATE_TYJDJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYJDYE")) {
            sql.execute("call SP_UPDATE_TYJDYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TZYWJY")) {
            sql.execute("call SP_UPDATE_TZYWJY('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TZYWYE")) {
            sql.execute("call SP_UPDATE_TZYWYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRDKJC")) {
            sql.execute("call SP_UPDATE_GRDKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRDKYE")) {
            sql.execute("call SP_UPDATE_GRDKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRDKFK")) {
            sql.execute("call SP_UPDATE_GRDKFK('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("WTDKJC")) {
            sql.execute("call SP_UPDATE_WTDKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("WTDKYE")) {
            sql.execute("call SP_UPDATE_WTDKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("WTDKFK")) {
            sql.execute("call SP_UPDATE_WTDKFK('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DGKHXX")) {
            sql.execute("call SP_UPDATE_DGKHXX('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWCKFS")) {
            sql.execute("call SP_UPDATE_DWCKFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWCKJC")) {
            sql.execute("call SP_UPDATE_DWCKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWCKYE")) {
            sql.execute("call SP_UPDATE_DWCKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("DWDKFK")) {
            sql.execute("call SP_UPDATE_DWDKFK('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("FTPDJB")) {
            sql.execute("call SP_UPDATE_FTPDJB('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRCKFS")) {
            sql.execute("call SP_UPDATE_GRCKFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRCKJC")) {
            sql.execute("call SP_UPDATE_GRCKJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("GRCKYE")) {
            sql.execute("call SP_UPDATE_GRCKYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("JGFRXX")) {
            sql.execute("call SP_UPDATE_JGFRXX('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("JGFZXX")) {
            sql.execute("call SP_UPDATE_JGFZXX('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("LCXCTJ")) {
            sql.execute("call SP_UPDATE_LCXCTJ('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("MRFSFS")) {
            sql.execute("call SP_UPDATE_MRFSFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("MRFSJC")) {
            sql.execute("call SP_UPDATE_MRFSJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("MRFSYE")) {
            sql.execute("call SP_UPDATE_MRFSYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("PJTXFS")) {
            sql.execute("call SP_UPDATE_PJTXFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("PJTXJC")) {
            sql.execute("call SP_UPDATE_PJTXJC('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("PJTXYE")) {
            sql.execute("call SP_UPDATE_PJTXYE('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYCKFS")) {
            sql.execute("call SP_UPDATE_TYCKFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TYJDFS")) {
            sql.execute("call SP_UPDATE_TYJDFS('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TZYWFQ")) {
            sql.execute("call SP_UPDATE_TZYWFQ('${dataRptDate}')");
        }
        if (tableName.startsWith("IMAS_PM_") && tableName.endsWith("TZYWZD")) {
            sql.execute("call SP_UPDATE_TZYWZD('${dataRptDate}')");
        }

        if (collectMode == "1") {
            // 变化量
            // 更新新增数据上报类型为 新增
            sw.start("mod collect update to A")
            def updateASql = cleanSpaces("""
                update ${tableName} a left join (select * from ${preTableName} where SJRQ = '$preDataRptDate') b on ${abPkEqCond}
                set a.DATA_RPT_FLAG = 'A'
                where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} and b.DATA_ID is null 
            """.toString())
            log.debug("mod collect update to A sql[$updateASql]")
            sql.executeUpdate(updateASql)
            sw.stop()
            def leftJoinSqlSegment = """
                left join (select t.* from ${preTableName} t where t.SJRQ = '$preDataRptDate') b 
                on ${abPkEqCond} and (_busCond_)
            """.toString()
            // 更新新增数据上报类型为 修改
            sw.start("mod collect update to M")
            def updateMSql = cleanSpaces("update ${tableName} a ${leftJoinSqlSegment} set a.DATA_RPT_FLAG = 'M' where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")}  and b.DATA_ID is not null".replace("_busCond_", busDiffCond).toString())
            log.debug("mod collect update to M sql[$updateMSql]")
            sql.executeUpdate(updateMSql)
            sw.stop()
            // 更新新增数据上报类型为 无变化
            sw.start("mod collect update to O")
            def updateOSql = cleanSpaces("update ${tableName} a ${leftJoinSqlSegment} set a.DATA_RPT_FLAG = 'O', a.CHECK_FLAG = b.CHECK_FLAG where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} and b.DATA_ID is not null".replace("_busCond_", busEqCond).toString())
            log.debug("mod collect update to O sql[$updateOSql]")
            sql.executeUpdate(updateOSql)
            sw.stop()
        } else {
            sw.start("add collect update to A")
            def updateASql = """
                update ${tableName}
                set DATA_RPT_FLAG = 'A'
                where SJRQ = '${dataRptDate}'
                  and NEXT_ACTION != '99' 
                  and ${cond.replace("alias.", "")} 
            """.toString()
            sql.executeUpdate(cleanSpaces(updateASql))
            sw.stop()
        }
        log.info(sw.prettyPrint())
        return Result.OK
    }

    Result sqlserver(String dataRptDate, String preDataRptDate, String tableName, String preTableName, String collectMode, List<String> pks, List<String> buses, String cond) {
        StopWatch sw = new StopWatch("sqlserver data compare for $tableName $dataRptDate")
        String abPkEqCond = pks.stream().map(pk -> " a.${pk} = b.${pk} ").collect(Collectors.joining(" and "))
        String busDiffCond = buses.stream().map(pk -> " a.${pk} != b.${pk} or (a.${pk} is null and b.${pk} is not null) or (a.${pk} is not null and b.${pk} is null)").collect(Collectors.joining(" or "))
        String busEqCond = buses.stream().map(pk -> " (a.${pk} = b.${pk} or (a.${pk} is null and b.${pk} is null)) ").collect(Collectors.joining(" and "))

        if (collectMode == "1") {
            // 变化量
            // 更新新增数据上报类型为 新增
            sw.start("mod collect update to A")
            def updateASql = cleanSpaces("""
                update a
                set a.DATA_RPT_FLAG = 'A'
                from ${tableName} a left join (select * from ${preTableName} where SJRQ = '$preDataRptDate') b on ${abPkEqCond}
                where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} and b.DATA_ID is null 
            """.toString())
            log.debug("mod collect update to A sql[$updateASql]")
            sql.executeUpdate(updateASql)
            sw.stop()
            def leftJoinSqlSegment = """
                left join (select t.* from ${preTableName} t where t.SJRQ = '$preDataRptDate') b 
                on ${abPkEqCond} and (_busCond_)
            """.toString()
            // 更新新增数据上报类型为 修改
            sw.start("mod collect update to M")
            def updateMSql = cleanSpaces("update a set a.DATA_RPT_FLAG = 'M' from ${tableName} a ${leftJoinSqlSegment} where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} and b.DATA_ID is not null".replace("_busCond_", busDiffCond).toString())
            log.debug("mod collect update to M sql[$updateMSql]")
            sql.executeUpdate(updateMSql)
            sw.stop()
            // 更新新增数据上报类型为 无变化
            sw.start("mod collect update to O")
            def updateOSql = cleanSpaces("update a set a.DATA_RPT_FLAG = 'O', a.CHECK_FLAG = b.CHECK_FLAG from ${tableName} a ${leftJoinSqlSegment} where a.SJRQ = '${dataRptDate}' and a.NEXT_ACTION!='99' and ${cond.replace("alias.", "a.")} and b.DATA_ID is not null".replace("_busCond_", busEqCond).toString())
            log.debug("mod collect update to O sql[$updateOSql]")
            sql.executeUpdate(updateOSql)
            sw.stop()
        } else {
            sw.start("add collect update to A")
            def updateASql = """
                update ${tableName}
                set DATA_RPT_FLAG = 'A'
                where SJRQ = '${dataRptDate}'
                  and NEXT_ACTION != '99'  
                  and ${cond.replace("alias.", "")}
            """.toString()
            sql.executeUpdate(cleanSpaces(updateASql))
            sw.stop()
        }
        log.info(sw.prettyPrint())
        return Result.OK
    }
}
