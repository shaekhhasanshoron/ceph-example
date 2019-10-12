package com.ceph.cephtest.dto.client.policy;

public class UpdateBucketPolicyDTO {
    private String bucketName;
    private String accessPolicy;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getAccessPolicy() {
        return accessPolicy;
    }

    public void setAccessPolicy(String accessPolicy) {
        this.accessPolicy = accessPolicy;
    }
}
