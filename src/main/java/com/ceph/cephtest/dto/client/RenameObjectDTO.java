package com.ceph.cephtest.dto.client;

public class RenameObjectDTO {
    private String bucketName;
    private String objectKey;
    private String newObjectName;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public void setObjectKey(String objectKey) {
        this.objectKey = objectKey;
    }

    public String getNewObjectName() {
        return newObjectName;
    }

    public void setNewObjectName(String newObjectName) {
        this.newObjectName = newObjectName;
    }
}
