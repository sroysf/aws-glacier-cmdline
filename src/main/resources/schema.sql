CREATE TABLE PUBLIC.UPLOAD (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    awsUploadId         VARCHAR(50),
    fileName            VARCHAR(100),
    fileHash            VARCHAR(100),
    vault               VARCHAR(50),
    json                VARCHAR(500),
    status              VARCHAR(15)
);

CREATE TABLE PUBLIC.UPLOAD_PART (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    upload_id           INTEGER,
    part_num            INTEGER,
    start_byte          BIGINT,
    end_byte            BIGINT,
    part_hash           VARCHAR(100),
    status              VARCHAR(15)
);