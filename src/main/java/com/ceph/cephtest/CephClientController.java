package com.ceph.cephtest;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import com.ceph.cephtest.dto.client.*;
import com.ceph.cephtest.dto.client.policy.UpdateBucketPolicyDTO;
import com.ceph.cephtest.dto.client.policy.UpdateDirectoryPolicyDTO;
import com.ceph.cephtest.dto.client.policy.UpdateObjectPolicyDTO;
import com.ceph.cephtest.services.CephClientService;
import com.ceph.cephtest.services.CephClientServiceV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@RestController
@RequestMapping("/ceph-client")
public class CephClientController {

    @Autowired
    CephClientService cephClientService;

    @Autowired
    CephClientServiceV2 cephClientServiceV2;

    @RequestMapping(value = "/create-bucket", method = RequestMethod.POST)
    public ResponseEntity<?> createBucket(@Valid @RequestBody CreateBucketDTO input) {
        ResponseDTO result = cephClientService.createBucket(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


    @RequestMapping(value = "/update-bucket-policy", method = RequestMethod.POST)
    public ResponseEntity<?> updateBucketPolicy(@Valid @RequestBody UpdateBucketPolicyDTO input) {
        ResponseDTO result = cephClientService.updateBucketPolicy(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-bucket-list", method = RequestMethod.GET)
    public ResponseEntity<?> getBuckets() {
        ResponseDTO result = cephClientService.getBuckets();
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/delete-bucket/{bucket-name}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteBucket( @PathVariable("bucket-name") String bucketName) {
        ResponseDTO result = cephClientService.deleteBucket(bucketName);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/create-directory", method = RequestMethod.POST)
    public ResponseEntity<?> createDirectory(@Valid @RequestBody CreateDirectoryDTO input) {
        ResponseDTO result = cephClientServiceV2.createDirectory(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


    @RequestMapping(value = "/copy-move-directory-objects", method = RequestMethod.POST)
    public ResponseEntity<?> copyMoveDirectoryObjects(@Valid @RequestBody CopyMoveDirectoryAndObjectsDTO input) {
        ResponseDTO result = cephClientServiceV2.copyMoveDirectoryAndObjects(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/rename-directory", method = RequestMethod.POST)
    public ResponseEntity<?> renameDirectory(@Valid @RequestBody RenameDirectoryDTO input) {
        ResponseDTO result = cephClientServiceV2.renameDirectory(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/delete-directory", method = RequestMethod.POST)
    public ResponseEntity<?> deleteDirectory(@Valid @RequestBody DeleteDirectoryDTO input) {
        ResponseDTO result = cephClientServiceV2.deleteDirectory(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/update-directory-policy", method = RequestMethod.POST)
    public ResponseEntity<?> updateDirectoryPolicy(@Valid @RequestBody UpdateDirectoryPolicyDTO input) {
        ResponseDTO result = cephClientServiceV2.updateDirectoryPolicy(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

//    @RequestMapping(value = "/upload-objects", method = RequestMethod.POST)
//    public ResponseEntity<?> uploadFilesToBucket(@Valid @RequestBody UploadObjectsToBucketDTO input) {
//        ResponseDTO result = cephClientService.uploadObjects(input);
//        return new ResponseEntity<>(result, HttpStatus.OK);
//    }


    @RequestMapping(value = "/update-object-policy", method = RequestMethod.POST)
    public ResponseEntity<?> updateObjectPolicy(@Valid @RequestBody UpdateObjectPolicyDTO input) {
        ResponseDTO result = cephClientServiceV2.updateObjectPolicy(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/upload-objects", method = RequestMethod.POST)
    public ResponseEntity<?> uploadFilesToBucket(@Valid @RequestBody UploadObjectsToBucketDTO input) {
        ResponseDTO result = cephClientServiceV2.uploadObjects(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


    @RequestMapping(value = "/rename-object", method = RequestMethod.POST)
    public ResponseEntity<?> renameObject(@Valid @RequestBody RenameObjectDTO input) {
        ResponseDTO result = cephClientServiceV2.renameObject(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

//    @RequestMapping(value = "/update-object", method = RequestMethod.POST)
//    public ResponseEntity<?> updateObject(@Valid @RequestBody UpdateObjectDTO input) {
//        ResponseDTO result = cephClientService.updateObject(input);
//        return new ResponseEntity<>(result, HttpStatus.OK);
//    }


    @RequestMapping(value = "/get-objects/{bucket-name}", method = RequestMethod.GET)
    public ResponseEntity<?> getFilesFromBucket(@PathVariable("bucket-name") String bucketName, @RequestParam("directory") String directoryUrl) {
        ResponseDTO result = cephClientService.getObjectList(bucketName, directoryUrl);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-object", method = RequestMethod.POST)
    public ResponseEntity<?> getObject(@Valid @RequestBody GetObjectDTO input) {
        ResponseDTO result = cephClientService.getObject(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-object-preview-link", method = RequestMethod.POST)
    public ResponseEntity<?> getObjectPreviewLink(@Valid @RequestBody GetObjectDTO input) {
        ResponseDTO result = cephClientService.getPreviewLink(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-download-link", method = RequestMethod.POST)
    public ResponseEntity<?> getDownloadLink(@Valid @RequestBody GetObjectDTO input) {
        ResponseDTO result = cephClientService.getDownloadLink(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/delete-directories-objects", method = RequestMethod.POST)
    public ResponseEntity<?> deleteDirectoryAndObjects(@Valid @RequestBody DeleteDirectoriesAndObjectDTO input) {
        ResponseDTO result = cephClientServiceV2.deleteDirectoriesAndObjects(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/delete-objects", method = RequestMethod.POST)
    public ResponseEntity<?> deleteFilesFromBucket(@Valid @RequestBody DeleteObjectsFromBucketDTO input) {
        ResponseDTO result = cephClientServiceV2.deleteObjects(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/download-object", method = RequestMethod.POST)
    public ResponseEntity<?> downloadObject(@Valid @RequestBody DownloadObjectDTO input) {
        ResponseDTO result = cephClientServiceV2.downloadObject(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }


    @RequestMapping(value = "/create-bucket-v2", method = RequestMethod.POST)
    public ResponseEntity<?> createBucketV2(@Valid @RequestBody CreateBucketDTO input) {

        AWSCredentials credentials = new BasicAWSCredentials("GXCNNMPE6VYRRV7SL7P5", "drMokVrwV1jsKFKSHKbQoUBkFMNTIHWyHIXGp9JI");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new
                AwsClientBuilder.EndpointConfiguration("http://rook-ceph-ext-obj-store.devxn.com:30010", "");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setEndpointConfiguration(endpointConfiguration);
        builder.withPathStyleAccessEnabled(true);
        builder.setClientConfiguration(clientConfig);
        builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        AmazonS3 conn = builder.build();



//        endpointConfiguration.withPathStyleAccessEnabled(true);
//        AmazonS3 conn = com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
//        .withCredentials(new AWSStaticCredentialsProvider(credentials))
//        .withClientConfiguration(clientConfig)
//        .withEndpointConfiguration(endpointConfiguration)
//        .build();

        try {
            Bucket bucket = null;
            if (!conn.doesBucketExistV2(input.getBucketName())) {
                conn.createBucket(input.getBucketName());
            }

            conn.putObject(
                    input.getBucketName(),
                    "Document/hello.txt",
                    new File("/Users/user/Document/hello.txt")
            );
//            Bucket bucket = conn.createBucket(input.getBucketName());
//            Boolean bucketExistence = conn.doesBucketExistV2(input.getBucketName());
            List<Bucket> buckets = conn.listBuckets();
            System.out.println(buckets);
            return new ResponseEntity<>(buckets, HttpStatus.OK);
        }  catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(e);
        }

        return new ResponseEntity<>(null, HttpStatus.OK);
    }

    @RequestMapping(value = "/upload-object", method = RequestMethod.POST)
    public ResponseEntity<?> uploadFile() {
        AWSCredentials credentials = new BasicAWSCredentials("GXCNNMPE6VYRRV7SL7P5", "drMokVrwV1jsKFKSHKbQoUBkFMNTIHWyHIXGp9JI");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new
                AwsClientBuilder.EndpointConfiguration("http://rook-ceph-ext-obj-store.devxn.com:30010", "");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setEndpointConfiguration(endpointConfiguration);
        builder.withPathStyleAccessEnabled(true);
        builder.setClientConfiguration(clientConfig);
        builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        AmazonS3 conn = builder.build();

        try {
//            File file = new File("C:\\abcfolder\\textfile.txt");
//            String existingBucketName = "bucketone";
//            String keyName = "wolverine-v2.jpg";
//
//            String filePath = "C://Users//shoro//Desktop//pics//wolv.jpg";
////            String amazonFileUploadLocationOriginal=existingBucketName+"/";
////            File file = new File(filePath);
//            FileInputStream stream = new FileInputStream(filePath);
////            InputStream is = new BufferedInputStream(stream);
////            String mimeType = URLConnection.guessContentTypeFromStream(is);
//
//
//            ObjectMetadata objectMetadata = new ObjectMetadata();
////            objectMetadata.setContentType(mimeType); //
//            PutObjectRequest putObjectRequest = new PutObjectRequest(existingBucketName, keyName, stream, objectMetadata);
//            PutObjectResult result = conn.putObject(putObjectRequest);
            File file = new File("C://Users//shoro//Desktop//pics//ymate.mp3");

            Path path = file.toPath();
            String mimeType = Files.probeContentType(path);

            //bFile will be the placeholder of file bytes
            byte[] bFile = new byte[(int) file.length()];

            ObjectMetadata md = new ObjectMetadata();

            InputStream myInputStream = new ByteArrayInputStream(bFile);
            md.setContentLength(bFile.length);
            md.setContentEncoding("UTF-8");
            if (mimeType.equals("audio/mpeg")) {
                md.setContentType("audio/mp3");
            } else {
                md.setContentType(mimeType);
            }

            PutObjectResult result = conn.putObject(new PutObjectRequest("bucketone", "ymate2.mp3", myInputStream, md));



//            String existingBucketName = "bucketone";
//            String keyName = "wolverine-v4.jpg";
//            String filePath = "C://Users//shoro//Desktop//pics//wolv.jpg";
//
//            FileInputStream stream = new FileInputStream(filePath);
//            InputStream is = new BufferedInputStream(stream);
//            String mimeType = URLConnection.guessContentTypeFromStream(is);
//            ObjectMetadata objectMetadata = new ObjectMetadata();
//            objectMetadata.setContentType(mimeType);
//            PutObjectRequest putObjectRequest = new PutObjectRequest(existingBucketName, keyName, stream, objectMetadata);
//            PutObjectResult result = conn.putObject(putObjectRequest);
            return new ResponseEntity<>(result, HttpStatus.OK);
        }  catch (SdkClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(e);
        }

        return new ResponseEntity<>(null, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-object-list", method = RequestMethod.POST)
    public ResponseEntity<?> getObjectList() {
        AWSCredentials credentials = new BasicAWSCredentials("GXCNNMPE6VYRRV7SL7P5", "drMokVrwV1jsKFKSHKbQoUBkFMNTIHWyHIXGp9JI");
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new
                AwsClientBuilder.EndpointConfiguration("http://rook-ceph-ext-obj-store.devxn.com:30010", "");
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setEndpointConfiguration(endpointConfiguration);
        builder.withPathStyleAccessEnabled(true);
        builder.setClientConfiguration(clientConfig);
        builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        AmazonS3 conn = builder.build();

        try {
//            File file = new File("C:\\abcfolder\\textfile.txt");
            String existingBucketName = "bucketone";
            ObjectListing objects = conn.listObjects(existingBucketName);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    System.out.println(objectSummary.getKey() + "\t" +
                            objectSummary.getSize() + "\t" +
                            StringUtils.fromDate(objectSummary.getLastModified()));
                }
                objects = conn.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());

//            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(existingBucketName, "superman.jpg");
//            System.out.println(conn.generatePresignedUrl(request));
//            return new ResponseEntity<>(conn.generatePresignedUrl(request), HttpStatus.OK);





            // Set the presigned URL to expire after one hour.
            java.util.Date expiration = new java.util.Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis += 1000 * 60 * 60;
            expiration.setTime(expTimeMillis);

            S3Object fullObject = conn.getObject(new GetObjectRequest(existingBucketName, "ymate2.mp3"));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());

            // Generate the presigned URL. for only viewing
            System.out.println("Generating pre-signed URL.");
            ResponseHeaderOverrides responseHeaders = new ResponseHeaderOverrides();
            responseHeaders.setContentType("application/octet-stream");

            GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(existingBucketName, "ymate2.mp3", HttpMethod.HEAD);
            req.setExpiration(expiration);
            req.setMethod(HttpMethod.GET);
            req.setResponseHeaders(responseHeaders);

            URL url = conn.generatePresignedUrl(req);
            System.out.println("Printing head url 1: " + url.toString());


            // Url FOr Downloading
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(existingBucketName, "ymate2.mp3")
                            .withMethod(HttpMethod.GET)
                            .withExpiration(expiration);
            URL url2 = conn.generatePresignedUrl(generatePresignedUrlRequest);

            System.out.println("Pre-Signed URL 2: " + url2.toString());
            return new ResponseEntity<>(url.toString(), HttpStatus.OK);
        }  catch (SdkClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(e);
        }
        return new ResponseEntity<>(null, HttpStatus.OK);
    }
}
