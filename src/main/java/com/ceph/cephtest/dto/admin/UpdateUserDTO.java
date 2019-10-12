package com.ceph.cephtest.dto.admin;

public class UpdateUserDTO {
    private String userId;
    private String userName;
    private String userEmail;
    private int maxBuckets;
    private long maxObjects;
    private long maxSize;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
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
}
