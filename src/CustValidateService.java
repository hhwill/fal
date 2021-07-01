package com.gingkoo.imas.hsbc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.gingkoo.imas.core.batch.ImasBatchBasicValidateService;
import com.gingkoo.root.facility.spring.tx.TransactionHelper;

@Component
public class CustValidateService {

    private ImasBatchBasicValidateService imasBatchBasicValidateService;

    private final JdbcTemplate jdbcTemplate;

    public CustValidateService(TransactionHelper transactionTemplate, DataSource dataSource,
                               ImasBatchBasicValidateService imasBatchBasicValidateService, CustLoadFileProcessService custLoadFileProcessService) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.imasBatchBasicValidateService = imasBatchBasicValidateService;
    }

    public void execute(String rptDate) {
        List<String> lst = new ArrayList<String>();
        try {
            List<Map<String, Object>> records = jdbcTemplate.queryForList("select GUID from gp_bm_id_filedata where " +
                    "length(guid)=6");
            for (Map<String, Object> record : records) {
                lst.add(record.get("GUID").toString());
            }
            try {
                imasBatchBasicValidateService.validate(rptDate, lst, null, null, null, true);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception exx) {
            exx.printStackTrace();
        }
    }
}
