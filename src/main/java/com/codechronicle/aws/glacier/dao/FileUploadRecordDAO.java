package com.codechronicle.aws.glacier.dao;

import com.codechronicle.aws.glacier.model.FileUploadRecord;
import com.codechronicle.aws.glacier.model.FileUploadStatus;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/23/12
 * Time: 2:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUploadRecordDAO extends BaseDAO {

    public FileUploadRecordDAO(DataSource dataSource) {
        super(dataSource);
    }

    public FileUploadRecord findByHash(String fileHash) throws SQLException {

        FileUploadRecord record = getQueryRunner().query("SELECT * FROM UPLOAD WHERE fileHash=?", new ResultSetHandler<FileUploadRecord>() {
            @Override
            public FileUploadRecord handle(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return mapFileRecord(rs);
                } else {
                    return null;
                }
            }
        }, fileHash);

        return record;
    }

    public List<FileUploadRecord> findByStatus(FileUploadStatus status) throws SQLException {
        List<FileUploadRecord> records = null;

        records = getQueryRunner().query("SELECT * FROM UPLOAD WHERE status=? ORDER BY id ASC", new ResultSetHandler<List<FileUploadRecord>>() {
            @Override
            public List<FileUploadRecord> handle(ResultSet rs) throws SQLException {
                List<FileUploadRecord> results = new ArrayList<FileUploadRecord>();
                while (rs.next()) {
                    results.add(mapFileRecord(rs));
                }
                return results;
            }
        }, status.toString());

        return records;
    }

    private FileUploadRecord mapFileRecord(ResultSet rs) throws SQLException {
        FileUploadRecord record = new FileUploadRecord();

        record.setId(rs.getInt("id"));
        record.setAwsUploadId(rs.getString("awsUploadId"));
        record.setFileHash(rs.getString("fileHash"));
        record.setFileName(rs.getString("fileName"));
        record.setJson(rs.getString("json"));
        record.setStatus(FileUploadStatus.valueOf(rs.getString("status")));
        record.setVault(rs.getString("vault"));
        record.setCreationDate(rs.getTimestamp("creationDate"));
        record.setCompletionDate(rs.getTimestamp("completionDate"));

        return record;
    }

    public void create(FileUploadRecord fileUploadRecord) throws SQLException {
        getQueryRunner().update("INSERT INTO UPLOAD (awsUploadId, fileHash, fileName, vault, json, status, creationDate) VALUES (?,?,?,?,?,?,?)",
                fileUploadRecord.getAwsUploadId(),
                fileUploadRecord.getFileHash(),
                fileUploadRecord.getFileName(),
                fileUploadRecord.getVault(),
                fileUploadRecord.getJson(),
                fileUploadRecord.getStatus().toString(),
                new GregorianCalendar().getTime());
    }

    public void update(FileUploadRecord fileUploadRecord) throws SQLException {
        getQueryRunner().update("UPDATE UPLOAD u SET awsUploadId=?, fileHash=?, fileName=?, vault=?, json=?, status=?, completionDate=? WHERE u.id=?",
                fileUploadRecord.getAwsUploadId(),
                fileUploadRecord.getFileHash(),
                fileUploadRecord.getFileName(),
                fileUploadRecord.getVault(),
                fileUploadRecord.getJson(),
                fileUploadRecord.getStatus().toString(),
                fileUploadRecord.getCompletionDate(),
                fileUploadRecord.getId());
    }
}
