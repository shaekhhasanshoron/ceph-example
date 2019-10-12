package com.ceph.cephtest.dto.client;

public class CreateDirectoryDTO {

    private String bucketName;
    private String currentDirectory;
    private String newDirectoryName;

    public CreateDirectoryDTO() {
    }

    public CreateDirectoryDTO(String bucketName, String currentDirectory, String newDirectoryName) {
        this.bucketName = bucketName;
        this.currentDirectory = currentDirectory;
        this.newDirectoryName = newDirectoryName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getCurrentDirectory() {
        return currentDirectory;
    }

    public void setCurrentDirectory(String currentDirectory) {
        this.currentDirectory = currentDirectory;
    }

    public String getNewDirectoryName() {
        return newDirectoryName;
    }

    public void setNewDirectoryName(String newDirectoryName) {
        this.newDirectoryName = newDirectoryName;
    }
}
