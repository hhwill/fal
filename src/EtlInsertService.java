package com.gingkoo.imas.hsbc.service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.gingkoo.imas.core.batch.ImasBatchBasicValidateService;
import com.gingkoo.root.facility.spring.tx.TransactionHelper;

@Component
public class EtlInsertService {

    private final Logger logger = LoggerFactory.getLogger(EtlInsertService.class);

    private final JdbcTemplate jdbcTemplate;

    private final TransactionHelper transactionTemplate;

    private ImasBatchBasicValidateService imasBatchBasicValidateService;

    public EtlInsertService(TransactionHelper transactionTemplate, DataSource dataSource, ImasBatchBasicValidateService imasBatchBasicValidateService) {
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.imasBatchBasicValidateService = imasBatchBasicValidateService;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean updateData(String sql, String now, List<List<String>> params) {
        String newsql = String.format(sql, now.substring(6,8));
        if (params != null && params.size() > 0) {
            try {
                jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement pstmt, int i) throws SQLException {
                        int index = 1;
                        List<String> param = params.get(i);
                        for (int j = 0; j < param.size(); j++) {
                            if (param.get(j) == null || param.get(j).equals("")) {
                                pstmt.setNull(index, Types.VARCHAR);
                            } else {
                                pstmt.setString(index, param.get(j));
                            }
                            index++;
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return params.size();
                    }
                });
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return true;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean insertData(String sql, String groupId, String user, List<List<String>> params) {
        String newsql = sql;
        if(params.size() > 0) {
            String now = params.get(0).get(0);
            newsql = String.format(sql, now.substring(6,8));
        }
        int times = params.size() / 1000;
        if (params.size() % 1000 != 0) {
            times += 1;
        }
        for (int i = 0; i < times; i++) {
            List<List<String>> currentUpdates = new ArrayList<List<String>>();
            if (i == times - 1) {
                for (int m = i * 1000; m < params.size(); m++) {
                    currentUpdates.add(params.get(m));
                }
            } else {
                for (int m = i * 1000; m < (i + 1) * 1000; m++) {
                    currentUpdates.add(params.get(m));
                }
            }
            batchInsert(newsql, groupId, user, currentUpdates);
        }
//        if (params.size() > 0) {
//            try {
//                String tableName = sql.substring(21, 27).toUpperCase();
//                List<String> lst = new ArrayList<String>();
//                lst.add(tableName);
//                imasBatchBasicValidateService.validate(params.get(0).get(0), lst, "HSBC", groupId, null, false);
//            } catch (Exception ex) {
//
//            }
//        }
//        if (params.size() > 0) {
//            try{
//                String tableName = sql.substring(13, 27);
//                String nbjgh = "update " + tableName + " a inner join map_nbjgh b on a.nbjgh = b.src set a.nbjgh = b.dest " +
//                    "where a.sjrq = '" + params.get(0).get(0) + "'";
//                jdbcTemplate.execute(nbjgh);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
        return true;
    }

    public boolean batchInsert(String sql, String groupId, String user, List<List<String>> params) {
        Date now = new Date();
        String date = new SimpleDateFormat("yyyyMMdd").format(now);
        String time = new SimpleDateFormat("yyyyMMddhhmmss").format(now);
        try {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement pstmt, int i) throws SQLException {
                    int index = 1;
                    List<String> param = params.get(i);
                    pstmt.setString(index, UUID.randomUUID().toString().replace("-", ""));
                    index++;
                    pstmt.setString(index, param.get(0));
                    index++;
                    pstmt.setString(index, groupId);
                    index++;
                    for (int j = 0; j < param.size(); j++) {
                        if (param.get(j) == null || param.get(j).equals("")) {
                            pstmt.setNull(index, Types.VARCHAR);
                        } else {
                            pstmt.setString(index, param.get(j));
                        }
                        index++;
                    }
                    pstmt.setString(index, user);
                    index++;
                    pstmt.setString(index, date);
                    index++;
                    pstmt.setString(index, time);
                }

                @Override
                public int getBatchSize() {
                    return params.size();
                }
            });
        } catch (Exception ex) {
            for (int i = 0; i < params.size(); i++) {
                try {
                    final List<String> param = params.get(i);
//                    transactionTemplate.run(Propagation.REQUIRES_NEW, () -> {
                        jdbcTemplate.update(sql, new PreparedStatementSetter() {
                            @Override
                            public void setValues(PreparedStatement pstmt) throws SQLException {
                                int index = 1;
                                pstmt.setString(index, UUID.randomUUID().toString().replace("-", ""));
                                index++;
                                pstmt.setString(index, param.get(0));
                                index++;
                                pstmt.setString(index, groupId);
                                index++;
                                for (int j = 0; j < param.size(); j++) {
                                    if (param.get(j) == null || param.get(j).equals("")) {
                                        pstmt.setNull(index, Types.VARCHAR);
                                    } else {
                                        pstmt.setString(index, param.get(j));
                                    }
                                    index++;
                                }
                                pstmt.setString(index, user);
                                index++;
                                pstmt.setString(index, date);
                                index++;
                                pstmt.setString(index, time);
                            }
                        });
//                    });
                } catch (Exception singleex) {
                    logger.info(params.get(i).toString());
                    singleex.printStackTrace();
                }
            }
        }
        return true;
    }

}
