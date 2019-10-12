package com.ceph.cephtest.dto.client;

import java.util.List;

public class DeleteDirectoriesAndObjectDTO {

    private String bucketName;
    private List<String> directoryUrlList;
    private List<String> objectKeyList;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public List<String> getDirectoryUrlList() {
        return directoryUrlList;
    }

    public void setDirectoryUrlList(List<String> directoryUrlList) {
        this.directoryUrlList = directoryUrlList;
    }

    public List<String> getObjectKeyList() {
        return objectKeyList;
    }

    public void setObjectKeyList(List<String> objectKeyList) {
        this.objectKeyList = objectKeyList;
    }
}
