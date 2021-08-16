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

