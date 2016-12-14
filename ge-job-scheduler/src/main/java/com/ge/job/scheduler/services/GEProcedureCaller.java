package com.ge.job.scheduler.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * Created by User on 13/12/2016.
 */

@Service
public class GEProcedureCaller {

    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public static final String FUNDING_AGGREGATE_AMOUNT = "SELECT COUNT(1) FROM ai ";

    @Autowired
    public GEProcedureCaller(DataSource dataSource) {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    public List<Map<String, Object>> getFundingAggregateAmount(){
        return namedParameterJdbcTemplate.query(FUNDING_AGGREGATE_AMOUNT, new MapSqlParameterSource(), new ColumnMapRowMapper());
    }

    public void loadCrunchBaseDataToCompanyProfile(){
        namedParameterJdbcTemplate.update("call loadCrunchBaseDataToCompanyProfile()", new MapSqlParameterSource());
    }
}
