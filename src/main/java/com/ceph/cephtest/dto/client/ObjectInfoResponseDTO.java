package com.ceph.cephtest.dto.client;

import java.util.Date;

public class ObjectInfoResponseDTO {
    private String fileKey;
    private String fileName;
    private String currentDirectory;
    private String url;
    private Date lastModifiedDate;
    private Date creationDate;
    private int objectSize;
    private String objectACL;

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public ObjectInfoResponseDTO() {
    }

    public ObjectInfoResponseDTO(String fileKey, String url) {
        this.fileKey = fileKey;
        this.url = url;
    }

    public ObjectInfoResponseDTO(String fileKey, String fileName, String url, Date lastModifiedDate, int objectSize) {
        this.fileKey = fileKey;
        this.fileName = fileName;
        this.url = url;
        this.lastModifiedDate = lastModifiedDate;
        this.objectSize = objectSize;
    }

    public String getObjectACL() {
        return objectACL;
    }

    public void setObjectACL(String objectACL) {
        this.objectACL = objectACL;
    }

    public String getCurrentDirectory() {
        return currentDirectory;
    }

    public void setCurrentDirectory(String currentDirectory) {
        this.currentDirectory = currentDirectory;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(int objectSize) {
        this.objectSize = objectSize;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }


    public String getFileKey() {
        return fileKey;
    }

    public void setFileKey(String fileKey) {
        this.fileKey = fileKey;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
