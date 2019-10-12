package com.ceph.cephtest.dto.admin;

public class CreateUserDTO {
    private String userName;
    private String userEmail;
    private String userId;
    private int maxBuckets = 1;
    private long maxObjects = 1000;
    private long maxSize = 1000000; // 1 GB

    public long getMaxObjects() {
        return maxObjects;
    }

    public void setMaxObjects(long maxObjects) {
        this.maxObjects = maxObjects;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public int getMaxBuckets() {
        return maxBuckets;
    }

    public void setMaxBuckets(int maxBuckets) {
        this.maxBuckets = maxBuckets;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
