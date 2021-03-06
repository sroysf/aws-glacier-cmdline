CREATE TABLE PUBLIC.UPLOAD (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    awsUploadId         VARCHAR(200),
    awsArchiveId        VARCHAR(200),
    fileName            VARCHAR(100),
    fileHash            VARCHAR(100),
    vault               VARCHAR(50),
    json                VARCHAR(500),
    status              VARCHAR(15),
    length              BIGINT,
    creationDate        DATETIME,
    completionDate      DATETIME
);

CREATE TABLE PUBLIC.UPLOAD_PART (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    uploadId            INTEGER,
    partNum             INTEGER,
    startByte           BIGINT,
    endByte             BIGINT,
    partHash            VARCHAR(100),
    completionDate      DATETIME
);