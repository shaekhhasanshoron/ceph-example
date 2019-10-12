package com.ceph.cephtest.dto.client;

public class RenameDirectoryDTO {

    private String bucketName;
    private String sourceDirectory;
    private String targetDirectoryName;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getSourceDirectory() {
        return sourceDirectory;
    }

    public void setSourceDirectory(String sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public String getTargetDirectoryName() {
        return targetDirectoryName;
    }

    public void setTargetDirectoryName(String targetDirectoryName) {
        this.targetDirectoryName = targetDirectoryName;
    }
}
