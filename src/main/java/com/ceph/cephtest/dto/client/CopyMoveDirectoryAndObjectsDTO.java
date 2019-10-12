package com.ceph.cephtest.dto.client;

import java.util.List;

public class CopyMoveDirectoryAndObjectsDTO {

    private String sourceBucketName;
    private String targetBucketName;
    private String targetDirectoryUrl;
    private List<String> sourceDirectoryUrlList;
    private List<String> sourceObjectKeyList;
    private String copyMoveAction;
    private boolean forceCopyMove = false;

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public void setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
    }

    public String getTargetBucketName() {
        return targetBucketName;
    }

    public void setTargetBucketName(String targetBucketName) {
        this.targetBucketName = targetBucketName;
    }

    public String getTargetDirectoryUrl() {
        return targetDirectoryUrl;
    }

    public void setTargetDirectoryUrl(String targetDirectoryUrl) {
        this.targetDirectoryUrl = targetDirectoryUrl;
    }

    public List<String> getSourceDirectoryUrlList() {
        return sourceDirectoryUrlList;
    }

    public void setSourceDirectoryUrlList(List<String> sourceDirectoryUrlList) {
        this.sourceDirectoryUrlList = sourceDirectoryUrlList;
    }

    public List<String> getSourceObjectKeyList() {
        return sourceObjectKeyList;
    }

    public void setSourceObjectKeyList(List<String> sourceObjectKeyList) {
        this.sourceObjectKeyList = sourceObjectKeyList;
    }

    public String getCopyMoveAction() {
        return copyMoveAction;
    }

    public void setCopyMoveAction(String copyMoveAction) {
        this.copyMoveAction = copyMoveAction;
    }

    public boolean isForceCopyMove() {
        return forceCopyMove;
    }

    public void setForceCopyMove(boolean forceCopyMove) {
        this.forceCopyMove = forceCopyMove;
    }
}
