package com.codechronicle.aws.glacier.dao;

import com.codechronicle.aws.glacier.model.FileUploadPart;
import com.codechronicle.aws.glacier.model.FileUploadRecord;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

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

    public List<FileUploadPart> findParts(FileUploadRecord fileUploadRecord) throws SQLException {
        List<FileUploadPart> parts = getQueryRunner().query("SELECT * FROM UPLOAD_PART WHERE uploadId = ?", new ResultSetHandler<List<FileUploadPart>>() {
            @Override
            public List<FileUploadPart> handle(ResultSet rs) throws SQLException {
                List<FileUploadPart> results = new ArrayList<FileUploadPart>();
                while (rs.next()) {
                    results.add(mapResultSet(rs));
                }
                return results;
            }
        }, fileUploadRecord.getId());

        return parts;
    }

    private FileUploadPart mapResultSet(ResultSet rs) throws SQLException {
        FileUploadPart part = new FileUploadPart();

        part.setId(rs.getInt("id"));
        part.setUploadId(rs.getInt("uploadId"));
        part.setPartNum(rs.getInt("partNum"));
        part.setStartByte(rs.getLong("startByte"));
        part.setEndByte(rs.getLong("endByte"));
        part.setPartHash(rs.getString("partHash"));
        part.setCompletionDate(rs.getTimestamp("completionDate"));

        return part;
    }
}
