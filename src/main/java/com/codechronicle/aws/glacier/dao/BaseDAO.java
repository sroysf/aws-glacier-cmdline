package com.codechronicle.aws.glacier.dao;

import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class BaseDAO {

    private QueryRunner queryRunner;

    public BaseDAO(DataSource dataSource) {
        this.queryRunner = new QueryRunner(dataSource);
    }

    protected QueryRunner getQueryRunner() {
        return queryRunner;
    }
}
