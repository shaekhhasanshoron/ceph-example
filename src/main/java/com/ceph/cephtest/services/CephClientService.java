package com.ceph.cephtest.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.ceph.cephtest.ResponseDTO;
import com.ceph.cephtest.dto.client.*;
import com.ceph.cephtest.dto.client.policy.UpdateBucketPolicyDTO;
import com.ceph.cephtest.enums.AccessPolicy;
import com.ceph.cephtest.enums.ResponseStatus;
import com.ceph.cephtest.utils.CephConnection;
import com.ceph.cephtest.utils.ObjectKeyName;
import org.apache.commons.lang3.EnumUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import org.twonote.rgwadmin4j.RgwAdmin;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

import static com.amazonaws.auth.policy.actions.S3Actions.SetBucketPolicy;

@Service
public class CephClientService {

    @Autowired
    CephConnection cephConnection;

    public ResponseDTO createBucket(CreateBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        Bucket bucket = null;
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name required");
            } else if (client.doesBucketExistV2(input.getBucketName())){
                return new ResponseDTO<>().generateErrorResponse("Bucket name already exists");
            }
            bucket = client.createBucket(input.getBucketName().toLowerCase().trim().replaceAll(" ","-"));
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
       return new ResponseDTO<>().generateSuccessResponse(bucket, "Bucket Successfully Created");
    }



    public ResponseDTO updateBucketPolicy(UpdateBucketPolicyDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name required");
            } else if (!client.doesBucketExistV2(input.getBucketName())){
                return new ResponseDTO<>().generateErrorResponse("Bucket Not found");
            }

            if (input.getAccessPolicy() == null) {
                return new ResponseDTO<>().generateErrorResponse("Access Policy Required");
            }
            if (!EnumUtils.isValidEnum(AccessPolicy.class, input.getAccessPolicy())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Access Policy");
            }

            AccessControlList acl = client.getBucketAcl(input.getBucketName());
            if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PUBLIC) {
                acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
                String publicPolicy = getPublicReadPolicyForObjectsUnderBucket(input.getBucketName());
                client.setBucketAcl(input.getBucketName(), acl);
                client.setBucketPolicy(input.getBucketName(), publicPolicy);
            } else if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PRIVATE) {
                acl.revokeAllPermissions(GroupGrantee.AllUsers);
                client.setBucketAcl(input.getBucketName(), acl);
                client.deleteBucketPolicy(input.getBucketName());
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Bucket policy has been successfully updated");
    }


    public ResponseDTO modifyBucket(CreateBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        Bucket bucket = null;
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name required");
            } else if (!client.doesBucketExistV2(input.getBucketName())){
                return new ResponseDTO<>().generateErrorResponse("Bucket Not found");
            }
//            AccessControlList acl = client.getBucketAcl(input.getBucketName());
//            Permission permission = Permission.valueOf(access);
//            acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
//            acl.revokeAllPermissions(GroupGrantee.AllUsers);
//            client.setBucketAcl(input.getBucketName(), acl);

//            List<Grant> grants = acl.getGrantsAsList();
//            for (Grant grant : grants) {
//                System.out.format("  %s: %s\n", grant.getGrantee().getIdentifier(),
//                        grant.getPermission().toString());
//            }
//            BucketPolicy bucket_policy = client.getBucketPolicy(input.getBucketName());
//            String policyText = bucket_policy.getPolicyText();

            String publicPolicy = getPublicReadPolicyForObjectsUnderBucket(input.getBucketName());
            setBucketPolicy(input.getBucketName(), publicPolicy, client);
            System.out.println("Permitterd");
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(bucket, "Bucket ACL Changed Successfully");
    }

    // Sets a public read policy on the bucket.
    public static String getPublicReadPolicyForObjectsUnderBucket(String bucket_name) {
        Policy bucket_policy = new Policy().withStatements(
                new Statement(Statement.Effect.Allow)
                        .withPrincipals(Principal.AllUsers)
                        .withActions(S3Actions.GetObject)
                        .withResources(new Resource(
                                "arn:aws:s3:::" + bucket_name + "/*")));
        return bucket_policy.toJson();
    }

    public static void setBucketPolicy(String bucket_name, String policy_text, AmazonS3 client) {
//        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        try {
            client.setBucketPolicy(bucket_name, policy_text);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }


















    public ResponseDTO getBuckets() {
        AmazonS3 client = cephConnection.setClientConnection();
        List<Bucket> bucketList;
        try {
            bucketList = client.listBuckets();
            RgwAdmin admin = cephConnection.setAdminConnection();
//            admin.getBucketInfo()
//            admin.setIndividualBucketQuota("robi-admin", "bucket-robi-hr-one", 10000, 1000000);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
       return new ResponseDTO<>().generateSuccessResponse(bucketList, "Bucket List Response");
    }

    public ResponseDTO deleteBucket(String bucketName) {
        AmazonS3 client = cephConnection.setClientConnection();
        RgwAdmin admin = cephConnection.setAdminConnection();
        try {
            admin.removeBucket(bucketName);
//            /*
//                Delete all objects from the bucket.
//                This is sufficient for un-versioned buckets. For versioned buckets, when you attempt to delete objects, Amazon S3 inserts
//                delete markers for all objects, but doesn't delete the object versions.
//                To delete objects from versioned buckets, delete all of the object versions before deleting
//                the bucket (see below for an example).
//             */
//            ObjectListing objectListing = client.listObjects(bucketName);
//            while (true) {
//                Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
//                while (objIter.hasNext()) {
//                    client.deleteObject(bucketName, objIter.next().getKey());
//                }
//
//                // If the bucket contains many objects, the listObjects() call
//                // might not return all of the objects in the first listing. Check to
//                // see whether the listing was truncated. If so, retrieve the next page of objects
//                // and delete them.
//                if (objectListing.isTruncated()) {
//                    objectListing = client.listNextBatchOfObjects(objectListing);
//                } else {
//                    break;
//                }
//            }
//
//            // Delete all object versions (required for versioned buckets).
//            VersionListing versionList = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
//            while (true) {
//                Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
//                while (versionIter.hasNext()) {
//                    S3VersionSummary vs = versionIter.next();
//                    client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
//                }
//
//                if (versionList.isTruncated()) {
//                    versionList = client.listNextBatchOfVersions(versionList);
//                } else {
//                    break;
//                }
//            }
            // After all objects and object versions are deleted, delete the bucket.
//            client.deleteBucket(bucketName);

        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Bucket Deleted Successfully");
    }

    /*
        All spaces in directory name replaced by $$
        It will create and add a temp-xyz.html which will be hidden from user
     */
    public ResponseDTO createDirectory(CreateDirectoryDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (input.getCurrentDirectory() == null || input.getNewDirectoryName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Current directory url and new directory name required");
            }

            String prefixDirectoryUrl = "";
            if (!input.getCurrentDirectory().equals("/") && !input.getCurrentDirectory().equals("")) {
                prefixDirectoryUrl = input.getCurrentDirectory().endsWith("/") ? input.getCurrentDirectory() : input.getCurrentDirectory() +"/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (!input.getCurrentDirectory().equals("/") && !input.getCurrentDirectory().equals("") && objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Source Directory Not Found");
            }

            String directoryName = input.getNewDirectoryName().trim().replaceAll(" +"," ").replaceAll(" ", "\\$\\$");
            String keyName;
            if (input.getCurrentDirectory().equals("") || input.getCurrentDirectory().equals("/")) {
                keyName = directoryName + "/tmp-xyz.html";
            } else {
                keyName = input.getCurrentDirectory().endsWith("/") ? input.getCurrentDirectory() + directoryName + "/tmp-xyz.html" :
                        input.getCurrentDirectory() +"/" + directoryName + "/tmp-xyz.html";
            }
            if (!client.doesObjectExist(input.getBucketName(), keyName)) {
                client.putObject(input.getBucketName(), keyName,"Directory");
            } else {
                return new ResponseDTO<>().generateErrorResponse("Directory name already exists");
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been created successfully");
    }


    public ResponseDTO updateDirectory(RenameDirectoryDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null || input.getSourceDirectory() == null || input.getTargetDirectoryName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name, Source Directory and updated name required");
            }
            if (input.getSourceDirectory().equals("/") || input.getSourceDirectory().equals("")) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory");
            }

            String[] inputSourceDirectorySplit = input.getSourceDirectory().split("/");
            String sourceDirectoryName = inputSourceDirectorySplit[inputSourceDirectorySplit.length - 1];
            String targetDirectoryName = input.getTargetDirectoryName().trim().replaceAll(" +", " ").replaceAll(" ", "\\$\\$");
            if (sourceDirectoryName.equals(targetDirectoryName)) {
                return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been updated successfully");
            }

            String sourceParentDirectory = "";
            for (int count = 0; count < inputSourceDirectorySplit.length - 1; count++) {
                sourceParentDirectory = sourceParentDirectory + inputSourceDirectorySplit [count] + "/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(sourceParentDirectory + sourceDirectoryName + "/");
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Source Directory");
            }

            ResponseDTO response = createDirectory(new CreateDirectoryDTO(input.getBucketName(), sourceParentDirectory, targetDirectoryName));
            if (response.getStatus() == ResponseStatus.error) {
                return new ResponseDTO<>().generateErrorResponse(response.getMessage());
            }
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    String[] sourceObjectNameSplit = objectSummary.getKey().split("/");
                    String[] inputKeySplit = input.getSourceDirectory().split("/");
                    String keyName = "";
                    for (int index = 0; index < sourceObjectNameSplit.length; index++) {
                        if (inputKeySplit.length <= index ) {
                            keyName = keyName + sourceObjectNameSplit[index] + (index < sourceObjectNameSplit.length - 1 ? "/" : "");
                        }
                    }
                    if (!objectSummary.getKey().equals(sourceParentDirectory + sourceDirectoryName + "/tmp-xyz.html")) {
                        client.copyObject(
                                input.getBucketName(),
                                objectSummary.getKey(),
                                input.getBucketName(),
                                sourceParentDirectory + targetDirectoryName + "/" + keyName
                        );
                    }
                    client.deleteObject(new DeleteObjectRequest(input.getBucketName(), objectSummary.getKey()));
                }
                objects = client.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been updated successfully");
    }



    public ResponseDTO deleteDirectory(DeleteDirectoryDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null || input.getDirectory() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name and directory Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }

            String prefixDirectoryUrl = "";
            if (!input.getDirectory().equals("/") && !input.getDirectory().equals("")) {
                prefixDirectoryUrl = input.getDirectory().endsWith("/") ? input.getDirectory() : input.getDirectory() +"/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (input.getDirectory().equals("/") || input.getDirectory().equals("") || objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory");
            }
            ArrayList<KeyVersion> keys = new ArrayList<>();
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    keys.add(new KeyVersion(objectSummary.getKey()));
                }
                objects = client.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
            DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(input.getBucketName());
            delObjReq.setKeys(keys);
            client.deleteObjects(delObjReq);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been deleted successfully");
    }

    /*
        File name spaces is replaces by %
     */
    public ResponseDTO uploadObjects(UploadObjectsToBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        ArrayList<String> savedFilesKeyValue = new ArrayList<>();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (input.getFilePathList() == null || input.getFilePathList().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("No objects selected");
            }


            String prefixDirectoryUrl = "";
            if (!input.getDirectoryUrl().equals("/") && !input.getDirectoryUrl().equals("")) {
                prefixDirectoryUrl = input.getDirectoryUrl().endsWith("/") ? input.getDirectoryUrl() : input.getDirectoryUrl() +"/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);


            if (input.getDirectoryUrl() == null ||
                    (!input.getDirectoryUrl().equals("") && !input.getDirectoryUrl().equals("/") && objects.getObjectSummaries().isEmpty())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory");
            }

            for (String filePath : input.getFilePathList()) {
                File file = new File(filePath);
                Path path = file.toPath();
                String mimeType = Files.probeContentType(path);

                String fileName = file.getName().trim().replaceAll(" +"," ").replaceAll(" ", "%");
                String keyValue;
                if (!input.getDirectoryUrl().equals("/") && !input.getDirectoryUrl().equals("")) {
                    String directoryName;
                    if (input.getDirectoryUrl().startsWith("/")) {
                        directoryName = input.getDirectoryUrl().substring(1);
                    } else {
                        directoryName = input.getDirectoryUrl().endsWith("/") ? input.getDirectoryUrl() : input.getDirectoryUrl() + "/";
                    }
                    keyValue = directoryName + fileName;
                } else {
                    keyValue = fileName;
                }

                Map<String,String> userMetadata = new HashMap<>();
                userMetadata.put("x-amz-meta-url", "shoron.com");
                userMetadata.put("x-amz-meta-filename", "Shaekh Hasan");
                PutObjectRequest request = new PutObjectRequest(input.getBucketName(), keyValue, file)
                        .withCannedAcl(CannedAccessControlList.PublicRead);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType(mimeType);
                metadata.setUserMetadata(userMetadata);
//                metadata.addUserMetadata("x-amz-meta-title", "klover-cloud-user");
                request.setMetadata(metadata);
                client.putObject(request);
                savedFilesKeyValue.add(keyValue);
            }
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            // Rollback
            deleteObjects(new DeleteObjectsFromBucketDTO(savedFilesKeyValue, input.getBucketName()));
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            // Rollback
            deleteObjects(new DeleteObjectsFromBucketDTO(savedFilesKeyValue, input.getBucketName()));
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Object Uploaded Successfully");
    }


    public ResponseDTO updateObject(UpdateObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getSourceBucketName() == null || input.getSourceKeyName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Source Bucket name and Source Key name required");
            }
            if (input.getTargetBucketName() == null || input.getUpdatedObjectName() == null || input.getTargetDirectory() == null) {
                return new ResponseDTO<>().generateErrorResponse("Destination Bucket name directory and Updated Key name required");
            }
            if (!client.doesBucketExistV2(input.getSourceBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Source Bucket not found");
            }
            if (!client.doesObjectExist(input.getSourceBucketName(), input.getSourceKeyName())) {
                return new ResponseDTO<>().generateErrorResponse("Source Object not found in source bucket");
            }
            if (!client.doesBucketExistV2(input.getTargetBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Destination Bucket not found");
            }
            String prefixDirectoryUrl = "";
            if (!input.getTargetDirectory().equals("/") && !input.getTargetDirectory().equals("")) {
                prefixDirectoryUrl = input.getTargetDirectory().endsWith("/") ? input.getTargetDirectory() : input.getTargetDirectory() +"/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getTargetBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);


            if (!input.getTargetDirectory().equals("") && !input.getTargetDirectory().equals("/") && objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Target Directory Not Found");
            }

            String[] sourceObjectNameSplit = input.getSourceKeyName().split("/");
            String sourceObjectName = sourceObjectNameSplit[sourceObjectNameSplit.length - 1];
            String updatedObjectName = input.getUpdatedObjectName().trim().replaceAll(" +", " ").replaceAll(" ", "%")
                    + sourceObjectName.substring(sourceObjectName.lastIndexOf("."));
            String targetKeyName = prefixDirectoryUrl + updatedObjectName;

            if (sourceObjectName.equals(updatedObjectName)
                    && targetKeyName.equalsIgnoreCase(input.getSourceKeyName())) {
                return new ResponseDTO<>().generateSuccessResponse(null, "Object Updated Successfully");
            }

            if (!targetKeyName.equalsIgnoreCase(input.getSourceKeyName()) || (targetKeyName.equalsIgnoreCase(input.getSourceKeyName()) && !input.getTargetBucketName().equalsIgnoreCase(input.getSourceBucketName()))) {
                if (!client.doesObjectExist(input.getTargetBucketName(), targetKeyName)) {
                    client.copyObject(
                            input.getSourceBucketName(),
                            input.getSourceKeyName(),
                            input.getTargetBucketName(),
                            targetKeyName
                    );
                    client.deleteObject(new DeleteObjectRequest(input.getSourceBucketName(), input.getSourceKeyName()));
                } else {
                    return new ResponseDTO<>().generateErrorResponse("Updated object name already exists in destination Bucket");
                }
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Object Updated Successfully");
    }

    public ResponseDTO getObject(GetObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        ObjectInfoResponseDTO objectInfoResponseDTO = new ObjectInfoResponseDTO();
        try {
            if (input.getBucketName() == null || input.getKeyName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name and key name required");
            }
            if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (client.doesObjectExist(input.getBucketName(), input.getKeyName())) {
                if (input.getKeyName().contains(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                    return new ResponseDTO<>().generateErrorResponse("Object not found");
                }
                AccessControlList acl = client.getObjectAcl(input.getBucketName(), input.getKeyName());
                List<Grant> grantList = acl.getGrantsAsList();
                Grantee grantee = grantList.get(0).getGrantee();
                Permission permission = grantList.get(0).getPermission();
                GetObjectRequest getObjectRequest = new GetObjectRequest(input.getBucketName(), input.getKeyName());
                S3Object object = client.getObject(getObjectRequest);
                objectInfoResponseDTO.setFileKey(object.getKey());
                objectInfoResponseDTO.setUrl(client.getUrl(input.getBucketName(), input.getKeyName()).toExternalForm());
                objectInfoResponseDTO.setLastModifiedDate(object.getObjectMetadata().getLastModified());
                objectInfoResponseDTO.setObjectSize((int) object.getObjectMetadata().getContentLength());
                objectInfoResponseDTO.setObjectACL(acl.toString());
            } else {
                return new ResponseDTO<>().generateErrorResponse("Object not found");
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(objectInfoResponseDTO, "Object Response");
    }



    public ResponseDTO getObjectList(String bucketName, String directoryUrl) {
        AmazonS3 client = cephConnection.setClientConnection();
        ArrayList<ObjectInfoResponseDTO> getObjectResponseList = new ArrayList<>();
        ArrayList<String> directoryNameList = new ArrayList<>();
        try {
            if (!client.doesBucketExistV2(bucketName)) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }

            String prefixDirectoryUrl = "";
            if (!directoryUrl.equals("/") && !directoryUrl.equals("")) {
                prefixDirectoryUrl = directoryUrl.endsWith("/") ? directoryUrl : directoryUrl +"/";
            }

//            directoryUrl = directoryUrl.equals("/") ? "" : directoryUrl;

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (!directoryUrl.equals("/") && !directoryUrl.equals("") && objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Directory not found");
            }

           do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
//                    System.out.println("filelink: " + client.getUrl(bucketName, objectSummary.getKey()).toExternalForm());
//                    System.out.println("filelink: " + client.getSi);
//                    System.out.println(generatePresignedUrl(bucketName, objectSummary.getKey(), client));

                    String keyName;
                    ObjectMetadata metadata = client.getObjectMetadata(bucketName, objectSummary.getKey());
                    Map<String, String> userMetadata = metadata.getUserMetadata();
                    if (!directoryUrl.equals("")) {
                        String[] sourceObjectNameSplit = objectSummary.getKey().split("/");
                        String[] inputKeySplit = directoryUrl.split("/");
                        keyName = "";
                        for (int index = 0; index < sourceObjectNameSplit.length; index++) {
                            if (inputKeySplit.length <= index ) {
                                keyName = keyName + sourceObjectNameSplit[index] + (index < sourceObjectNameSplit.length -1 ? "/" : "");
                            }
                        }
                    } else {
                        keyName = objectSummary.getKey();
                    }
                    String[] directories = keyName.split("/");
                    if (directories.length > 1) {
                        String directoryName = directories[0];
                        if (!directoryNameList.contains(directoryName)) {
                            directoryNameList.add(directoryName);
                        }
                    } else {
                        String fileName = userMetadata.get(ObjectKeyName.OBJECT_FILE_NAME);
                        if (!fileName.equals(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) { // tmp-xyz.html is a temporary file which is created by system while creating directory
//                            OffsetDateTime creationDate = OffsetDateTime.parse(userMetadata.get(ObjectKeyName.OBJECT_CREATION_DATE_TIME));
//                            LocalDateTime creationDate = LocalDateTime.parse(userMetadata.get(ObjectKeyName.OBJECT_CREATION_DATE_TIME));
                            ObjectInfoResponseDTO objectInfoResponseDTO = new ObjectInfoResponseDTO();
                            objectInfoResponseDTO.setFileKey(objectSummary.getKey());
                            objectInfoResponseDTO.setCurrentDirectory(userMetadata.get(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL));
                            objectInfoResponseDTO.setFileName(fileName);
//                            objectInfoResponseDTO.setCreationDate(Date.from(creationDate.atZone( ZoneId.systemDefault()).toInstant()));
                            objectInfoResponseDTO.setUrl(client.getUrl(bucketName, objectSummary.getKey()).toExternalForm());
                            objectInfoResponseDTO.setLastModifiedDate(objectSummary.getLastModified());
                            objectInfoResponseDTO.setObjectSize((int) objectSummary.getSize());
                            getObjectResponseList.add(objectInfoResponseDTO);
                        }
                    }
                }
                objects = client.listNextBatchOfObjects(objects);
            } while (objects.isTruncated());
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(new GetObjectResponseDTO(directoryNameList, getObjectResponseList), "Object List Response");
    }


    public ResponseDTO getPreviewLink(GetObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null || input.getKeyName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name and key name required");
            }
            if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (client.doesObjectExist(input.getBucketName(), input.getKeyName())) {
                return new ResponseDTO<>().generateSuccessResponse(generatePresignedUrl(input.getBucketName(), input.getKeyName(), client), "Preview Link");
            } else {
                return new ResponseDTO<>().generateErrorResponse("Object not found");
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
    }

    public ResponseDTO getDownloadLink(GetObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null || input.getKeyName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket name and key name required");
            }
            if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (client.doesObjectExist(input.getBucketName(), input.getKeyName())) {
                return new ResponseDTO<>().generateSuccessResponse(generatePresignedUrlForDownloading(input.getBucketName(), input.getKeyName(), client), "Download Link");
            } else {
                return new ResponseDTO<>().generateErrorResponse("Object not found");
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
    }

    public ResponseDTO deleteObjects(DeleteObjectsFromBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        ArrayList<KeyVersion> keys = new ArrayList<>();
        try {
            if (input.getKeyNameList() == null || input.getKeyNameList().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Now key found with the Id");
            }
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            for (String keyName : input.getKeyNameList()) {
                if (client.doesObjectExist(input.getBucketName(), keyName)) {
                    keys.add(new KeyVersion(keyName));
                } else {
                    return new ResponseDTO<>().generateErrorResponse("Object not found");
                }
            }
            DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(input.getBucketName());
            delObjReq.setKeys(keys);
            client.deleteObjects(delObjReq);
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Files are successfully deleted");
    }

    private String generatePresignedUrl(String bucketName, String keyName, AmazonS3 client) {
        try {
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, keyName)
                            .withMethod(HttpMethod.GET);
            URL url = client.generatePresignedUrl(generatePresignedUrlRequest);
//            System.out.println("Pre-Signed URL 2: " + url.toString());
            return url.toString();
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private String generatePresignedUrlWithExpiration(String bucketName, String keyName, AmazonS3 client) {
        // Set the presigned URL to expire after one hour.
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60; // one hour
        expiration.setTime(expTimeMillis);

        try {
            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, keyName)
                            .withMethod(HttpMethod.GET)
                            .withExpiration(expiration);
            URL url = client.generatePresignedUrl(generatePresignedUrlRequest);
            return url.toString();
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String generatePresignedUrlForDownloading(String bucketName, String keyName, AmazonS3 client) {
        // Set the presigned URL to expire after one hour.
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60; // one hour
        expiration.setTime(expTimeMillis);

        try {
            ResponseHeaderOverrides responseHeaders = new ResponseHeaderOverrides();
            responseHeaders.setContentType("application/octet-stream");

            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, keyName)
                            .withMethod(HttpMethod.GET)
                            .withExpiration(expiration)
                            .withResponseHeaders(responseHeaders);
            URL url = client.generatePresignedUrl(generatePresignedUrlRequest);

            System.out.println("Pre-Signed URL For Download: " + url.toString());
            return url.toString();
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getErrorMessage(String errorCode) {
        if (errorCode.equalsIgnoreCase("TooManyBuckets")) {
            return "User reached maximum bucket limits. Please buy more buckets.";
        } else if (errorCode.equalsIgnoreCase("AccessDenied")) {
            return "User do not have access";
        }  else if (errorCode.equalsIgnoreCase("BucketAlreadyExists") || errorCode.equalsIgnoreCase("BucketAlreadyOwnedByYou")) {
            return "Bucket Name Already Exists. Please change the bucket name";
        } else if (errorCode.equalsIgnoreCase("BucketNotEmpty")) {
            return "Bucket is not empty. Please delete all objects before deleting bucket";
        } else if (errorCode.equalsIgnoreCase("CredentialsNotSupported")) {
            return "Credentials Not Supported";
        } else if (errorCode.equalsIgnoreCase("InvalidBucketName")) {
            return "Invalid Bucket Name";
        } else if (errorCode.equalsIgnoreCase("InvalidAccessKeyId")) {
            return "Invalid Access Key Id";
        } else if (errorCode.equalsIgnoreCase("NoSuchBucket")) {
            return "Bucket does not exists";
        } else if (errorCode.equalsIgnoreCase("SignatureDoesNotMatch")) {
            return "Invalid Access Key/Secret key";
        } else if (errorCode.equalsIgnoreCase("BadDigest")) {
            return "Problem in file";
        }
        return "Access Denied";
    }
}
