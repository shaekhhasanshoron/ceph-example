package com.ceph.cephtest;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.User;

import java.util.List;

@RestController
public class WelcomeController {

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String helloWorld() {
        return "Welcome Shoron";
    }

    @RequestMapping(value = "/connect", method = RequestMethod.GET)
    public void checkCephConnection() {
//            AWSCredentials credentials = new BasicAWSCredentials("admin", "bfPsS1NttF");
        String accessKey = "admin";
        String secretKey = "rook-ceph-dashboard-admin-password";
//        String secretKey = "bfPsS1NttF";

        // Our firewall on DEV does some weird stuff so we disable SSL cert check
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        if (SDKGlobalConfiguration.isCertCheckingDisabled()) {
            System.out.println("Cert checking is disabled");
        }

        // S3 Client configuration
        ClientConfiguration config = new ClientConfiguration();
        // Not the standard "AWS3SignerType", maar expliciet signerTypeV2
        config.setSignerOverride("S3SignerType");
        config.setProtocol(Protocol.HTTPS);
//        config.setProxyHost("rook-ceph.cloudslip.io");

//        config.setProxyPort(8080);
        // S3 Credentials
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        // S3 Endpoint
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new
                AwsClientBuilder.EndpointConfiguration("rook-ceph.cloudslip.io", "");
        AmazonS3 s3 = com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(endpointConfiguration)
                .build();

        System.out.println("===========================================");
        System.out.println(" Connection to the Rubix S3 ");
        System.out.println("===========================================\n");
        try {
            /*
             * List of buckets and objects in our account
             */
            System.out.println("Listing buckets and objects");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName() + " "
                        + "(owner = " + bucket.getOwner()
                        + " "
                        + "(creationDate = " + bucket.getCreationDate());
                ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                        .withBucketName(bucket.getName()));
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    System.out.println(" --- " + objectSummary.getKey() + " "
                            + "(size = " + objectSummary.getSize() + ")" + " "
                            + "(eTag = " + objectSummary.getETag() + ")");
                    System.out.println();
                }
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it to S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code: " + ase.getErrorCode());
            System.out.println("Error Type: " + ase.getErrorType());
            System.out.println("Request ID: " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3,"
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    @RequestMapping(value = "/connect2", method = RequestMethod.GET)
    public void checkCephConnection2() {
        try{
//            AWSCredentials credentials = new BasicAWSCredentials("admin", "bfPsS1NttF");
//            AWSCredentials credentials = new BasicAWSCredentials("UAY082141EF9128IALBV", "ecD911DSOYRtceoZkRTkthjGyA9vsNI2KZPCNrBW");
//
//            ClientConfiguration clientConfig = new ClientConfiguration();
//            clientConfig.setProtocol(Protocol.HTTPS);
//
//            AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
//            conn.setEndpoint("https://rook-ceph-ext-obj-store.devxn.com:30010");
//
//            List<Bucket> buckets = conn.listBuckets();
//            System.out.println(buckets);


            String accessKey = "UAY082141EF9128IALBV";
            String secretKey = "ecD911DSOYRtceoZkRTkthjGyA9vsNI2KZPCNrBW";

            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonS3 conn = new AmazonS3Client(credentials);
            conn.setEndpoint("http://rook-ceph-ext-obj-store.devxn.com:30010");

            List<Bucket> buckets = conn.listBuckets();
            System.out.println(buckets);

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    @RequestMapping(value = "/admin", method = RequestMethod.GET)
    public void checkCephAdminConnection() {
        try{
            RgwAdmin RGW_ADMIN = new RgwAdminBuilder()
                            .accessKey("shoron-access-key")
                            .secretKey("shoron-secret-key")
                            .endpoint("http://rook-ceph-ext-obj-store.devxn.com:30010/admin")
                            .build();
            List<User> users = RGW_ADMIN.listUserInfo();
            System.out.println(users);
        } catch (Exception e) {
            System.out.println(e);

        }
    }
}
