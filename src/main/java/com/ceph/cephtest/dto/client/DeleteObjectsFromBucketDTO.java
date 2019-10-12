package com.ceph.cephtest.dto.client;

import java.util.List;

public class DeleteObjectsFromBucketDTO {

    private List<String> keyNameList;
    private String bucketName;

    public DeleteObjectsFromBucketDTO() {
    }

    public DeleteObjectsFromBucketDTO(List<String> keyNameList, String bucketName) {
        this.keyNameList = keyNameList;
        this.bucketName = bucketName;
    }

    public List<String> getKeyNameList() {
        return keyNameList;
    }

    public void setKeyNameList(List<String> keyNameList) {
        this.keyNameList = keyNameList;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}
