package com.codechronicle.aws.glacier.dao;

import com.codechronicle.aws.glacier.model.FileUploadPart;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.GregorianCalendar;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 5:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUploadPartDAO extends BaseDAO {

    public FileUploadPartDAO(DataSource dataSource) {
        super(dataSource);
    }

    public int findMaxSuccessfulPartNumber(int fileUploadId) throws SQLException {

        int maxPartNum = getQueryRunner().query("SELECT MAX(partNum) AS maxPartNum FROM PUBLIC.UPLOAD_PART WHERE id=?", new ResultSetHandler<Integer>() {
            @Override
            public Integer handle(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return rs.getInt("maxPartNum");
                } else {
                    return 0;
                }
            }
        }, fileUploadId);

        return maxPartNum;
    }

    public void create(FileUploadPart part) throws SQLException {
        getQueryRunner().update("INSERT INTO UPLOAD_PART (uploadId, partNum, startByte, endByte, partHash, completionDate) VALUES (?,?,?,?,?,?)",
                part.getUploadId(),
                part.getPartNum(),
                part.getStartByte(),
                part.getEndByte(),
                part.getPartHash(),
                new GregorianCalendar().getTime());
    }
}
