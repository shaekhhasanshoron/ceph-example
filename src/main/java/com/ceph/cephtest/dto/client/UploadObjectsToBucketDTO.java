package com.ceph.cephtest.dto.client;

import java.util.List;

public class UploadObjectsToBucketDTO {
    private List<String> filePathList;
    private String directoryUrl;
    private String bucketName;
    private boolean forceUpload = false;

    public boolean isForceUpload() {
        return forceUpload;
    }

    public void setForceUpload(boolean forceUpload) {
        this.forceUpload = forceUpload;
    }

    public String getDirectoryUrl() {
        return directoryUrl;
    }

    public void setDirectoryUrl(String directoryUrl) {
        this.directoryUrl = directoryUrl;
    }

    public List<String> getFilePathList() {
        return filePathList;
    }

    public void setFilePathList(List<String> filePathList) {
        this.filePathList = filePathList;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}
