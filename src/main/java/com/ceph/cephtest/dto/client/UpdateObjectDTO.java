package com.ceph.cephtest.dto.client;

public class UpdateObjectDTO {
    private String sourceBucketName;
    private String sourceKeyName;
    private String targetBucketName;
    private String targetDirectory;
    private String updatedObjectName;

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
    }

    public String getSourceKeyName() {
        return sourceKeyName;
    }

    public void setSourceKeyName(String sourceKeyName) {
        this.sourceKeyName = sourceKeyName;
    }

    public String getTargetBucketName() {
        return targetBucketName;
    }

    public void setTargetBucketName(String targetBucketName) {
        this.targetBucketName = targetBucketName;
    }

    public String getTargetDirectory() {
        return targetDirectory;
    }

    public void setTargetDirectory(String targetDirectory) {
        this.targetDirectory = targetDirectory;
    }

    public String getUpdatedObjectName() {
        return updatedObjectName;
    }

    public void setUpdatedObjectName(String updatedObjectName) {
        this.updatedObjectName = updatedObjectName;
    }
}
