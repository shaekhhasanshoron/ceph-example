package com.ceph.cephtest.dto.client;

public class DeleteDirectoryDTO {

    private String bucketName;
    private String directory;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }
}
