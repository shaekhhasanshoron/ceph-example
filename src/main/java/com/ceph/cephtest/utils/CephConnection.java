package com.ceph.cephtest.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY;

@Service
public class CephConnection {

    @Value("${ceph.client.url}")
    private String cephClientServerEndpoint;

    @Value("${ceph.client.access}")
    private String cephClientAccessKey;

    @Value("${ceph.client.secret}")
    private String cephClientSecretKey;

    @Value("${ceph.admin.url}")
    private String  cephAdminEndpoint;

    @Value("${ceph.admin.access}")
    private String cephAdminAccessKey;

    @Value("${ceph.admin.secret}")
    private String cephAdminSecretKey;

    public AmazonS3 setClientConnection() {
        AWSCredentials credentials = new BasicAWSCredentials(cephClientAccessKey, cephClientSecretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxErrorRetry(DEFAULT_MAX_ERROR_RETRY);
        clientConfig.setRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                DEFAULT_BACKOFF_STRATEGY, DEFAULT_MAX_ERROR_RETRY, false));

        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(cephClientServerEndpoint, "");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setEndpointConfiguration(endpointConfiguration);
        builder.withPathStyleAccessEnabled(true);
        builder.setClientConfiguration(clientConfig);
        builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        AmazonS3 conn = builder.build();
        return conn;
    }

    public RgwAdmin setAdminConnection() {
        RgwAdmin RGW_ADMIN = new RgwAdminBuilder()
                .accessKey(cephAdminAccessKey)
                .secretKey(cephAdminSecretKey)
                .endpoint(cephAdminEndpoint)
                .build();
        return RGW_ADMIN;
    }
}
