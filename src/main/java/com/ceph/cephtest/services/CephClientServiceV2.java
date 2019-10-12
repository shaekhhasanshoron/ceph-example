package com.ceph.cephtest.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.ceph.cephtest.ResponseDTO;
import com.ceph.cephtest.dto.client.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.ceph.cephtest.dto.client.policy.UpdateDirectoryPolicyDTO;
import com.ceph.cephtest.dto.client.policy.UpdateObjectPolicyDTO;
import com.ceph.cephtest.enums.AccessPolicy;
import com.ceph.cephtest.enums.ResponseStatus;
import com.ceph.cephtest.utils.CephConnection;
import com.ceph.cephtest.utils.ObjectKeyName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.EnumUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.twonote.rgwadmin4j.model.Quota;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Service
public class CephClientServiceV2 {

    @Value("${ceph.client.userId}")
    private String cephClientUserId;

    @Autowired
    CephConnection cephConnection;

    @Autowired
    CephAdminService cephAdminService;

    @Autowired
    private ObjectMapper objectMapper;



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
            if (!checkDirectoryAndObjectNamePattern(input.getNewDirectoryName())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Name for new Directory");
            }
            if (!input.getCurrentDirectory().equals("") && !checkDirectoryUrlPattern(input.getCurrentDirectory())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Source Directory Url");
            }

            String newDirectoryName =  input.getNewDirectoryName().trim().replaceAll(" +", " ");
            String currentDirectoryUrl =  input.getCurrentDirectory().trim().replaceAll(" +", " ");

            String prefixDirectoryUrl = "";
            if (!currentDirectoryUrl.equals("")) {
                prefixDirectoryUrl = currentDirectoryUrl +"/";
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);
            if (!input.getCurrentDirectory().equals("") && objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Source Directory Not Found");
            }

            String keyNameForTempObject = newDirectoryName + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE;
            if (!currentDirectoryUrl.equals("")) {
                keyNameForTempObject = currentDirectoryUrl + "/" + keyNameForTempObject;
            }


            if (!client.doesObjectExist(input.getBucketName(), keyNameForTempObject)) {
                Map<String,String> userMetadata = new HashMap<>();
                userMetadata.put(ObjectKeyName.OBJECT_CREATION_DATE_TIME, String.valueOf(LocalDateTime.now()));
                userMetadata.put(ObjectKeyName.OBJECT_FILE_NAME, ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE);
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_NAME, newDirectoryName);
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL, (currentDirectoryUrl.equals("") ? "" : currentDirectoryUrl + "/") + newDirectoryName);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setUserMetadata(userMetadata);
                InputStream inputStream = new ByteArrayInputStream(new byte[0]); // For Temporary file
                PutObjectRequest request = new PutObjectRequest(input.getBucketName(), keyNameForTempObject, inputStream, metadata)
                                                    .withCannedAcl(CannedAccessControlList.Private);
                client.putObject(request);
            } else {
                return new ResponseDTO<>().generateErrorResponse("Directory name already exists");
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        }  catch (SdkClientException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Client Error");
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been created successfully");
    }

    public ResponseDTO copyMoveDirectoryAndObjects(CopyMoveDirectoryAndObjectsDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(client)
                .build();
        try {
            if (input.getSourceBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name required");
            } else if (!client.doesBucketExistV2(input.getSourceBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Source Bucket does not exists");
            }

            if (input.getTargetBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name required");
            } else if (!client.doesBucketExistV2(input.getTargetBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Source Bucket does not exists");
            }

            if (input.getCopyMoveAction() == null || (!input.getCopyMoveAction().equalsIgnoreCase(ObjectKeyName.OBJECT_COPY_KEYWORD) && !input.getCopyMoveAction().equalsIgnoreCase(ObjectKeyName.OBJECT_MOVE_KEYWORD))) {
                return new ResponseDTO<>().generateErrorResponse("Invalid action type");
            }

            /*
                Check Target Directory Url
             */
            if (input.getTargetDirectoryUrl() == null) {
                return new ResponseDTO<>().generateErrorResponse("Target Directory Required");
            } else if (!input.getTargetDirectoryUrl().equals("") && !checkDirectoryUrlPattern(input.getTargetDirectoryUrl())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Target Directory Url");
            }

            String targetDirectoryUrl = input.getTargetDirectoryUrl().replaceAll(" +", " ");
            if (!targetDirectoryUrl.equals("") && !client.doesObjectExist(input.getTargetBucketName(), targetDirectoryUrl+ "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                return new ResponseDTO<>().generateErrorResponse("Target directory not Found");
            }

            /*
                Check directory
             */
            if (input.getSourceDirectoryUrlList() == null) {
                return new ResponseDTO<>().generateErrorResponse("Source Directory List cannot be null");
            }

            List<String> warningMessageList = new ArrayList<>();

            /*
                For each Copied Directory check and get each file and folders under that directory
             */
            Map<String, List<CopyMoveDirectoryObjectHelperDTO>> directoryCopyHelperMap = new HashMap<>();
            List<CopyMoveDirectoryObjectHelperDTO> copyMoveDirectoryHelperList = new ArrayList<>();
            for (String sourceDirectoryUrl : input.getSourceDirectoryUrlList()) {
                sourceDirectoryUrl = sourceDirectoryUrl.replaceAll(" +", " ");
                if (sourceDirectoryUrl.equals("") && !checkDirectoryUrlPattern(sourceDirectoryUrl)) {
                    return new ResponseDTO<>().generateErrorResponse("Invalid Source Directory Url");
                }
                if (!client.doesObjectExist(input.getSourceBucketName(), sourceDirectoryUrl + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("Source Directory '%s' not found", sourceDirectoryUrl));
                }
                ListObjectsRequest listRequest = new ListObjectsRequest()
                        .withBucketName(input.getSourceBucketName())
                        .withPrefix(sourceDirectoryUrl + "/");
                ObjectListing objectsInSourceDirectory = client.listObjects(listRequest);

                do {
                    for (S3ObjectSummary objectSummary : objectsInSourceDirectory.getObjectSummaries()) {
                        String[] sourceFullObjectKeySplit = objectSummary.getKey().split("/");
                        String[] sourceCopyDirectoryUrlSplit = sourceDirectoryUrl.split("/");
                        String sourceCopyDirectoryName = sourceCopyDirectoryUrlSplit[sourceCopyDirectoryUrlSplit.length - 1];
                        String sourceObjectName = sourceFullObjectKeySplit[sourceFullObjectKeySplit.length - 1];

                        String remainingObjectKeyFromDirectory = "";
                        for (int index = 0; index < sourceFullObjectKeySplit.length; index++) {
                            if (sourceCopyDirectoryUrlSplit.length  <= index) {
                                remainingObjectKeyFromDirectory = remainingObjectKeyFromDirectory + sourceFullObjectKeySplit[index] + (index < sourceFullObjectKeySplit.length - 1 ? "/" : "");
                            }
                        }

                        /*
                            Checking whether source directory name already exists in target directory
                         */
                        if (!input.isForceCopyMove()
                                && remainingObjectKeyFromDirectory.equals(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)
                                && client.doesObjectExist(input.getTargetBucketName(), (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/")+ sourceCopyDirectoryName + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                            String warningMessage = String.format("Directory '%s' already exists", (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/") + sourceCopyDirectoryName);
                            if (!warningMessageList.contains(warningMessage)) {
                                warningMessageList.add(warningMessage);
                            }
                        }

                        /*
                            This list will contain four values
                                1. objectFullKeyName, f.e "directory-one/d2/xyz.jpg"
                                2. objectKeyFromCopiedDirectory, f.e if we want to copy "directory-one" then this value will be "d2/xyz.jpg"
                                3. copiedDirectoryName, f.e if we want to copy "directory-one" then value will be "directory-one"
                                4. objectName, this will be "xyz.jpg"
                         */
                        copyMoveDirectoryHelperList.add(new CopyMoveDirectoryObjectHelperDTO(objectSummary.getKey(), remainingObjectKeyFromDirectory, sourceCopyDirectoryName, sourceObjectName));
                    }
                    directoryCopyHelperMap.put(sourceDirectoryUrl,copyMoveDirectoryHelperList);
                    objectsInSourceDirectory = client.listNextBatchOfObjects(objectsInSourceDirectory);
                } while (objectsInSourceDirectory.isTruncated());
            }


            /*
                Check Objects List
             */
            if (input.getSourceObjectKeyList() == null) {
                return new ResponseDTO<>().generateErrorResponse("Source Object List cannot be null");
            }


            /*
                For each selected object check files and get warning message
             */
            for (String sourceObjectKey : input.getSourceObjectKeyList()) {
                if (!client.doesObjectExist(input.getSourceBucketName(), sourceObjectKey)) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("Source Object key '%s' Not Found", sourceObjectKey));
                }
                String[] sourceObjectKeySplit = sourceObjectKey.split("/");
                String sourceObjectName = sourceObjectKeySplit[sourceObjectKeySplit.length - 1];

                if (!input.isForceCopyMove()
                        && client.doesObjectExist(input.getTargetBucketName(), (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/")+ sourceObjectName)) {
                    String warningMessage = String.format("Object name '%s' already exists", sourceObjectName);
                    if (!warningMessageList.contains(warningMessage)) {
                        warningMessageList.add(warningMessage);
                    }
                }
            }

            /*
                Warning message for duplicate directory
             */
            if (!input.isForceCopyMove() && !warningMessageList.isEmpty()) {
                return new ResponseDTO<>().generateWarningResponse(warningMessageList);
            }


            /*
                Copy Objects and Directories
             */
            for (Map.Entry<String, List<CopyMoveDirectoryObjectHelperDTO>> directoryHelperMap : directoryCopyHelperMap.entrySet()) {
                String[] sourceCopyDirectoryUrlSplit = directoryHelperMap.getKey().split("/");
                String copiedDirectoryName = sourceCopyDirectoryUrlSplit[sourceCopyDirectoryUrlSplit.length - 1];
                int count = 0;
                while (true) {
                    if (client.doesObjectExist(input.getTargetBucketName(),
                            (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/") + copiedDirectoryName + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                        copiedDirectoryName = copiedDirectoryName + "(" + ++count + ")";
                    } else {
                        break;
                    }
                }
                for (CopyMoveDirectoryObjectHelperDTO helper : directoryHelperMap.getValue()) {

                /*
                    Copy/Move Directory Objects to Target Directory
                 */
                    String newObjectKeyName = (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/") + copiedDirectoryName + "/" + helper.getObjectKeyFromCopiedDirectory();
                    ObjectMetadata metadata = client.getObjectMetadata(input.getSourceBucketName(), helper.getObjectFullKeyName());
                    Map<String, String> userMetadata = metadata.getUserMetadata();
                    if (!helper.getObjectKeyFromCopiedDirectory().contains("/")) {
                        userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_NAME, copiedDirectoryName);
                    }
                    userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL, newObjectKeyName.substring(0, newObjectKeyName.lastIndexOf("/")));
                    CopyObjectRequest request = new CopyObjectRequest(input.getSourceBucketName(), helper.getObjectFullKeyName(), input.getTargetBucketName(), newObjectKeyName)
                            .withNewObjectMetadata(metadata);
                    Copy copy = transferManager.copy(request);
                    copy.waitForCompletion();
                    if (input.getCopyMoveAction().equalsIgnoreCase(ObjectKeyName.OBJECT_MOVE_KEYWORD)) {
                        client.deleteObject(new DeleteObjectRequest(input.getSourceBucketName(), helper.getObjectFullKeyName()));
                    }
                }
            }

            /*
                Copy/Move Objects
             */
            for (String sourceObjectKey : input.getSourceObjectKeyList()) {
                ObjectMetadata metadata = client.getObjectMetadata(input.getSourceBucketName(), sourceObjectKey);
                Map<String, String> userMetadata = metadata.getUserMetadata();
                String objectFullName = userMetadata.get(ObjectKeyName.OBJECT_FILE_NAME);
                int count = 0;
                /*
                    Checking available object name in new directory
                 */
                while (true) {
                    if (client.doesObjectExist(input.getTargetBucketName(),
                            (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/") + objectFullName)) {
                        if (objectFullName.contains(".")) {
                            String[] objectNameSplit = objectFullName.split("\\.");
                            objectFullName = objectNameSplit[0] + "(" + ++count + ")" + "." + objectNameSplit[1];
                        } else {
                            objectFullName = objectFullName + "(" + ++count + ")";
                        }
                    } else {
                        break;
                    }
                }

                String[] targetDirectorySplit = targetDirectoryUrl.split("/");
                String targetDirectoryName = targetDirectorySplit[targetDirectorySplit.length - 1];

                userMetadata.put(ObjectKeyName.OBJECT_FILE_NAME, objectFullName);
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_NAME, targetDirectoryName);
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL, targetDirectoryUrl);
                CopyObjectRequest request = new CopyObjectRequest(input.getSourceBucketName(), sourceObjectKey, input.getTargetBucketName(), (targetDirectoryUrl.equals("") ? "" : targetDirectoryUrl + "/") + objectFullName)
                        .withNewObjectMetadata(metadata);
                Copy copy = transferManager.copy(request);
                copy.waitForCompletion();
                if (input.getCopyMoveAction().equalsIgnoreCase(ObjectKeyName.OBJECT_MOVE_KEYWORD)) {
                    client.deleteObject(new DeleteObjectRequest(input.getSourceBucketName(), sourceObjectKey));
                }
            }

        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        transferManager.shutdownNow();
        return new ResponseDTO<>().generateSuccessResponse(null, "Copied successfully");
    }


    public ResponseDTO renameDirectory(RenameDirectoryDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(client)
                .build();
        try {
            if (input.getBucketName() == null || input.getSourceDirectory() == null || input.getTargetDirectoryName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name, Source Directory and updated name required");
            }
            if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (!checkDirectoryAndObjectNamePattern(input.getTargetDirectoryName())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Name of New Directory");
            }
            if (input.getSourceDirectory().equals("") || !checkDirectoryUrlPattern(input.getSourceDirectory())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory Url");
            }

            String newDirectoryName =  input.getTargetDirectoryName().trim().replaceAll(" +", " ");
            String sourceDirectoryUrl =  input.getSourceDirectory().trim().replaceAll(" +", " ");

            /*
                Getting source directory name
             */
            String[] inputSourceDirectorySplit = sourceDirectoryUrl.split("/");
            String sourceDirectoryName = inputSourceDirectorySplit[inputSourceDirectorySplit.length - 1];
            if (sourceDirectoryName.equals(newDirectoryName)) {
                return new ResponseDTO<>().generateSuccessResponse(null, "Directory has been updated successfully");
            }

            /*
                Get parent directory url.
                For example. if url is directory-one/directory five/dir.
                Now if i want to change "directory five" then parent url should be directory-one
             */
            String parentUrlOfSourceDirectory = "";
            for (int index = 0; index < inputSourceDirectorySplit.length - 1; index++) {
                parentUrlOfSourceDirectory = parentUrlOfSourceDirectory + inputSourceDirectorySplit[index] + (index < inputSourceDirectorySplit.length - 2 ? "/" : "");
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix((parentUrlOfSourceDirectory.equals("") ? "" : parentUrlOfSourceDirectory + "/") + sourceDirectoryName + "/");
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Source Directory Not Found");
            }

            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    /*
                        The following portion will get the remaing part of a key
                        for example if a file under key is "directory one/directory one one/directory/xyz.jpg"
                            and want to rename "directory one one" to "directory two" then
                        There will be two parts one is parent url which will be "directory one"
                            and remaining part  "directory/xyz.jpg"
                     */
                    String[] sourceObjectNameSplit = objectSummary.getKey().split("/"); // Split the source object key
                    String[] inputKeySplit = input.getSourceDirectory().split("/"); //
                    String remainingKeyName = "";
                    for (int index = 0; index < sourceObjectNameSplit.length; index++) {
                        if (inputKeySplit.length <= index ) {
                            remainingKeyName = remainingKeyName + sourceObjectNameSplit[index] + (index < sourceObjectNameSplit.length - 1 ? "/" : "");
                        }
                    }
                    String newObjectKeyName = (parentUrlOfSourceDirectory.equals("") ? "" : parentUrlOfSourceDirectory + "/") + newDirectoryName + "/" + remainingKeyName;
                    ObjectMetadata metadata = client.getObjectMetadata(input.getBucketName(), objectSummary.getKey());
                    Map<String, String> userMetadata = metadata.getUserMetadata();
                    userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_NAME, newDirectoryName);
                    userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL, newObjectKeyName.substring(0, newObjectKeyName.lastIndexOf("/")));
                    CopyObjectRequest request = new CopyObjectRequest(input.getBucketName(), objectSummary.getKey(), input.getBucketName(), newObjectKeyName)
                            .withNewObjectMetadata(metadata);
                    Copy copy = transferManager.copy(request);
                    copy.waitForCompletion();
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
        transferManager.shutdownNow();
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
            if (!input.getDirectory().equals("") && !checkDirectoryUrlPattern(input.getDirectory())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Source Directory Url");
            }

            String prefixDirectoryUrl = "";
            if (!input.getDirectory().equals("")) {
                prefixDirectoryUrl = input.getDirectory() +"/";
            } else {
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory");
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(prefixDirectoryUrl);
            ObjectListing objects = client.listObjects(listObjectsRequest);

            if (objects.getObjectSummaries().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Directory Not Found");
            }
            ArrayList<KeyVersion> keys = new ArrayList<>();
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
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

    public ResponseDTO updateDirectoryPolicy(UpdateDirectoryPolicyDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (input.getDirectoryUrl() == null) {
                return new ResponseDTO<>().generateErrorResponse("Directory Url Required");
            }

            String directoryUrl = input.getDirectoryUrl().trim().replaceAll(" +", " ");
            if (!directoryUrl.equals("") && !checkDirectoryUrlPattern(directoryUrl)) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Source Directory Url");
            } else if (!client.doesObjectExist(input.getBucketName(), directoryUrl + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE) ) {
                return new ResponseDTO<>().generateErrorResponse("Directory Not Found");
            }

            if (input.getAccessPolicy() == null) {
                return new ResponseDTO<>().generateErrorResponse("Access Policy Required");
            } else if (!EnumUtils.isValidEnum(AccessPolicy.class, input.getAccessPolicy())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Access Policy");
            }

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(input.getBucketName())
                    .withPrefix(directoryUrl + "/");
            ObjectListing objects = client.listObjects(listObjectsRequest);
            do {
                for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                    if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PUBLIC) {
                        if (!objectSummary.getKey().contains(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                            client.setObjectAcl(input.getBucketName(), objectSummary.getKey(), CannedAccessControlList.PublicRead);
                        }
                    }
                    else if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PRIVATE) {
                        client.setObjectAcl(input.getBucketName(), objectSummary.getKey(), CannedAccessControlList.Private);
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

            return new ResponseDTO<>().generateSuccessResponse(null, "Directory policy has been successfully updated");
    }


    public ResponseDTO updateObjectPolicy(UpdateObjectPolicyDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }

            if (input.getObjectKey() == null) {
                return new ResponseDTO<>().generateErrorResponse("Object Key Required");
            } else if (!client.doesObjectExist(input.getBucketName(), input.getObjectKey())) {
                return new ResponseDTO<>().generateErrorResponse("Object Not Found");
            }


            if (input.getAccessPolicy() == null) {
                return new ResponseDTO<>().generateErrorResponse("Access Policy Required");
            } else if (!EnumUtils.isValidEnum(AccessPolicy.class, input.getAccessPolicy())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid Access Policy");
            }


            if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PUBLIC) {
                if (!input.getObjectKey().contains(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                    client.setObjectAcl(input.getBucketName(), input.getObjectKey(), CannedAccessControlList.PublicRead);
                }
            }
            else if (AccessPolicy.valueOf(input.getAccessPolicy()) == AccessPolicy.PRIVATE) {
                client.setObjectAcl(input.getBucketName(), input.getObjectKey(), CannedAccessControlList.Private);
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Object policy has been successfully updated");
    }

    // Sets a public read policy on the bucket.
    public static String getPublicReadPolicyForObjectsUnderDirectory(String bucket_name, String directoryUrl) {
        Policy bucket_policy = new Policy().withStatements(
                new Statement(Statement.Effect.Allow)
                        .withPrincipals(Principal.AllUsers)
                        .withActions(S3Actions.GetObject)
                        .withResources(new Resource(
                                "arn:aws:s3:::" + bucket_name + "/" + directoryUrl + "/*")));
        return bucket_policy.toJson();
    }

    /*

     */
    public ResponseDTO uploadObjects(UploadObjectsToBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(client)
                .build();

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
            if (input.getDirectoryUrl() == null) {
                return new ResponseDTO<>().generateErrorResponse("Directory Url cannot be null");
            }

            String directoryUrl = input.getDirectoryUrl().replaceAll(" +", " ");
            if (!directoryUrl.equals("") && !checkDirectoryUrlPattern(directoryUrl)){
                return new ResponseDTO<>().generateErrorResponse("Invalid Directory");
            } else if (!directoryUrl.equals("") && !client.doesObjectExist(input.getBucketName(), directoryUrl + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                return new ResponseDTO<>().generateErrorResponse("Directory not found");
            }

            String[] directorySplit = directoryUrl.split("/");
            String directoryName = directorySplit[directorySplit.length - 1];

            List<File> fileList = new ArrayList<>();
            List<String> warningMessageList = new ArrayList<>();
            double totalSizeOfFilesInKB = 0;

            /*
                Check For warning and Get Files
             */
            for (String path : input.getFilePathList()) {
                File file = new File(path);
                byte[] fileContent = Files.readAllBytes(file.toPath());
                if (fileContent.length < 1) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("File '%s' not found", file.getName()));
                }
                if (!input.isForceUpload() && client.doesObjectExist(input.getBucketName(), (directoryUrl.equals("") ? "" : directoryUrl + "/") + file.getName())) {
                    warningMessageList.add(String.format("File name '%s' already exists", file.getName()));
                }
                totalSizeOfFilesInKB = totalSizeOfFilesInKB + ((double) file.length() / 1024);
                fileList.add(file);
            }
            ResponseDTO getUserQuotaResponse = cephAdminService.getUserAvailableFreeSpace(cephClientUserId);
            if (getUserQuotaResponse.getStatus() == ResponseStatus.error) {
                return new ResponseDTO<>().generateErrorResponse(getUserQuotaResponse.getMessage());
            }

            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            long availableFreeSpaceInKB = objectMapper.convertValue(getUserQuotaResponse.getData(), new TypeReference<Long>() { });

            if (availableFreeSpaceInKB != Long.MAX_VALUE && availableFreeSpaceInKB < totalSizeOfFilesInKB) {
                return new ResponseDTO<>().generateErrorResponse("User do not have sufficient free space");
            }
            if (!input.isForceUpload() && !warningMessageList.isEmpty()) {
                return new ResponseDTO<>().generateWarningResponse(warningMessageList);
            }

            for (File file : fileList) {
                String mimeType = Files.probeContentType(file.toPath());
                String fileName = file.getName();

                /*
                    Checking available object name in new directory
                 */
                while (true) {
                    int count = 0;
                    if (client.doesObjectExist(input.getBucketName(),
                            (directoryUrl.equals("") ? "" : directoryUrl + "/") + fileName)) {
                        if (fileName.contains(".")) {
                            String[] objectNameSplit = fileName.split("\\.");
                            fileName = objectNameSplit[0] + "(" + ++count + ")"+ "." + objectNameSplit[1];
                        } else {
                            fileName = fileName + "(" + ++count + ")";
                        }
                    } else {
                        break;
                    }
                }

                Map<String, String> userMetadata = new HashMap<>();
                userMetadata.put(ObjectKeyName.OBJECT_FILE_NAME, fileName);
                userMetadata.put(ObjectKeyName.OBJECT_CREATION_DATE_TIME, String.valueOf(LocalDateTime.now()));
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_NAME, directoryName);
                userMetadata.put(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL, directoryUrl);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setUserMetadata(userMetadata);
                metadata.setContentType(mimeType);

//                PutObjectRequest request = new PutObjectRequest(input.getBucketName(), (directoryUrl.equals("") ? "" : directoryUrl + "/") + fileName, file)
//                        .withCannedAcl(CannedAccessControlList.PublicRead);
//                s3.setObjectAcl(bucketName, key, acl);
                PutObjectRequest request = new PutObjectRequest(input.getBucketName(), (directoryUrl.equals("") ? "" : directoryUrl + "/") + fileName, file)
                        .withCannedAcl(CannedAccessControlList.Private);
                request.setMetadata(metadata);
                Upload upload = transferManager.upload(request);

                while (!upload.isDone()) {
                    System.out.print(upload.getProgress().getBytesTransferred() + "/" + upload.getProgress().getTotalBytesToTransfer()
                            + " bytes transferred of " + fileName + " ....\r");
                    Thread.sleep(1000);
                    System.out.print(upload.getProgress().getBytesTransferred() + "/" + upload.getProgress().getTotalBytesToTransfer()
                            + " bytes transferred of " + fileName + " ....\n");
                }
                savedFilesKeyValue.add((directoryUrl.equals("") ? "" : directoryUrl + "/") +fileName);
            }
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            // Rollback
            deleteObjects(new DeleteObjectsFromBucketDTO(savedFilesKeyValue, input.getBucketName()));
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (NoSuchFileException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("No such file found : " + e.getMessage() );
        } catch (Exception e) {
            e.printStackTrace();
            // Rollback
            deleteObjects(new DeleteObjectsFromBucketDTO(savedFilesKeyValue, input.getBucketName()));
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        transferManager.shutdownNow();
        return new ResponseDTO<>().generateSuccessResponse(null, "Object Uploaded Successfully");
    }

    public ResponseDTO deleteDirectoriesAndObjects(DeleteDirectoriesAndObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        ArrayList<KeyVersion> keys = new ArrayList<>();
        try {
            if (input.getBucketName() == null || input.getDirectoryUrlList() == null || input.getObjectKeyList() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name, Directory Url list and object list cannot be null");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }
            if (input.getDirectoryUrlList().isEmpty() && input.getObjectKeyList().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Nothing is selected");
            }

            for (String directoryUrl : input.getDirectoryUrlList()) {
                if (directoryUrl.equals("") || !checkDirectoryUrlPattern(directoryUrl)) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("Invalid Directory Url '%s'", directoryUrl));
                }
                if (!client.doesObjectExist(input.getBucketName(), directoryUrl + "/" + ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("Directory '%s' not found", directoryUrl));
                }
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                        .withBucketName(input.getBucketName())
                        .withPrefix(directoryUrl + "/");
                ObjectListing objects = client.listObjects(listObjectsRequest);
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        keys.add(new KeyVersion(objectSummary.getKey()));
                    }
                    objects = client.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            }

            for (String objectKey : input.getObjectKeyList()) {
                if (!client.doesObjectExist(input.getBucketName(), objectKey)) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("Object key '%s' not found", objectKey));
                }
                keys.add(new KeyVersion(objectKey));
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
        return new ResponseDTO<>().generateSuccessResponse(null, "Successfully deleted");
    }

    public ResponseDTO deleteObjects(DeleteObjectsFromBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        ArrayList<KeyVersion> keys = new ArrayList<>();
        try {
            if (input.getKeyNameList() == null || input.getKeyNameList().isEmpty()) {
                return new ResponseDTO<>().generateErrorResponse("Key list is required");
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


    public ResponseDTO renameObject(RenameObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(client)
                .build();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }

            if (input.getObjectKey() == null ) {
                return new ResponseDTO<>().generateErrorResponse("Source Object Key required");
            } else if (!client.doesObjectExist(input.getBucketName(), input.getObjectKey())) {
                return new ResponseDTO<>().generateErrorResponse("Object Not Found");
            }

            if (input.getNewObjectName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Update Object name required");
            } else if (!checkDirectoryAndObjectNamePattern(input.getNewObjectName())) {
                return new ResponseDTO<>().generateErrorResponse("Invalid object name");
            }

            String newObjectName = input.getNewObjectName().replaceAll(" +", " ");
            ObjectMetadata metadata = client.getObjectMetadata(input.getBucketName(), input.getObjectKey());
            Map<String, String> userMetadata = metadata.getUserMetadata();
            String objectFullName = userMetadata.get(ObjectKeyName.OBJECT_FILE_NAME);
            String objectDirectoryUrl = userMetadata.get(ObjectKeyName.OBJECT_CURRENT_DIRECTORY_URL);
            if (objectFullName.equals(ObjectKeyName.TEMPORARY_FILE_NAME_FOR_DIRECTORY_CREATE)) {
                return new ResponseDTO<>().generateErrorResponse("File not found");
            }
            String[] objectNameSplit = objectFullName.split("\\.");
            String objectName = objectNameSplit[0];
            String objectExtension = objectNameSplit[1];
            if (objectName.equals(newObjectName)) {
                return new ResponseDTO<>().generateSuccessResponse(null, "File name updated successfully");
            }
            newObjectName = newObjectName + "." + objectExtension;
            userMetadata.put(ObjectKeyName.OBJECT_FILE_NAME, newObjectName);
            CopyObjectRequest request = new CopyObjectRequest(input.getBucketName(), input.getObjectKey(), input.getBucketName(), (objectDirectoryUrl.equals("") ? "" : objectDirectoryUrl + "/") + newObjectName)
                    .withNewObjectMetadata(metadata);
            Copy copy = transferManager.copy(request);
            copy.waitForCompletion();
            client.deleteObject(new DeleteObjectRequest(input.getBucketName(), input.getObjectKey()));
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        transferManager.shutdownNow();
        return new ResponseDTO<>().generateSuccessResponse(null, "File name updated successfully");
    }


    public ResponseDTO downloadObject(DownloadObjectDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(client)
                .build();
        try {
            if (input.getBucketName() == null) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Name Required");
            } else if (!client.doesBucketExistV2(input.getBucketName())) {
                return new ResponseDTO<>().generateErrorResponse("Bucket does not exists");
            }

            if (input.getObjectKey() == null ) {
                return new ResponseDTO<>().generateErrorResponse("Source Object Key required");
            } else if (!client.doesObjectExist(input.getBucketName(), input.getObjectKey())) {
                return new ResponseDTO<>().generateErrorResponse("Object Key Not Found");
            }

            ObjectMetadata objectMetadata = client.getObjectMetadata(input.getBucketName(), input.getObjectKey());
            Map<String, String> userMetadata = objectMetadata.getUserMetadata();
            File inputFilePath = new File(input.getDownloadFilePath() + "/" + userMetadata.get(ObjectKeyName.OBJECT_FILE_NAME));
            Download download = transferManager.download(input.getBucketName(), input.getObjectKey(), inputFilePath);
            // or block with Transfer.waitForCompletion()
            download.waitForCompletion();
        } catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse(getErrorMessage(e.getErrorCode()));
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        transferManager.shutdownNow();
        return new ResponseDTO<>().generateSuccessResponse(null, "Object downloaded successfully");
    }







    private boolean checkDirectoryUrlPattern(String directoryUrl) {
        String directoryPattern = "^(?=[^>~'\";,\\\\.|+&%#@$?:\\*\\=\\(\\)\\^\\!\\<]*$)(?!.*?--)(?!.*?__)(?!.*?-_)(?!.*?_-)(?!.*?\\/\\/)[a-zA-Z][\\s*a-zA-Z0-9-_\\/]*[a-zA-Z0-9]$";
        Matcher m = Pattern.compile(directoryPattern).matcher(directoryUrl);
        return m.matches();
    }
    /*
        Check the ingress pattern
    */
    private boolean checkDirectoryAndObjectNamePattern(String directoryName) {
        String pattern = "^(?=[^>~'\";,\\.|+&%#@$?:\\/\\\\*\\=\\(\\)\\^\\!\\<]*$)(?!.*?--)(?!.*?__)(?!.*?-_)(?!.*?_-)[a-zA-Z][\\s*a-zA-Z0-9-_]*[a-zA-Z0-9]$";
        Matcher m = Pattern.compile(pattern).matcher(directoryName);
        return m.matches();
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

   /*
        *********************************END*****************************************************
    */


    /*
    File name spaces is replaces by %
 */
    public ResponseDTO uploadObjectsas(UploadObjectsToBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            ArrayList<File> files = new ArrayList<File>();
//            for (String path : input.getFilePathList()) {
//                files.add(new File(path));
//            }

            TransferManager xfer_mgr = TransferManagerBuilder.standard()
                                    .withS3Client(client)
                                    .withMinimumUploadPartSize((long)(5*1024*1024))
                                    .withMultipartUploadThreshold((long)(5*1024*1024))
                                    .build();

//            File f = new File(file_path);
            for (String path : input.getFilePathList()) {
//                files.add(new File(path));
                File file = new File(path);
                try {
                    Map<String,String> userMetadata = new HashMap<>();
                    userMetadata.put("x-amz-meta-url", "shoron.com");
                    userMetadata.put("x-amz-meta-filename", "Shaekh Hasan");
                    PutObjectRequest request = new PutObjectRequest(input.getBucketName(), input.getDirectoryUrl()+ "/" + file.getName(), file)
                            .withCannedAcl(CannedAccessControlList.PublicRead);
                    ObjectMetadata metadata = new ObjectMetadata();
//                    metadata.setContentType(mimeType);
                    metadata.setUserMetadata(userMetadata);
//                metadata.addUserMetadata("x-amz-meta-title", "klover-cloud-user");
                    request.setMetadata(metadata);


                    Upload xfer = xfer_mgr.upload(request);

                    while (!xfer.isDone()) {
                        System.out.print(xfer.getProgress()
                                .getBytesTransferred()
                                + "/"
                                + xfer.getProgress()
                                .getTotalBytesToTransfer()
                                + " bytes transferred of "+ file.getName() +" ....\r");
                        Thread.sleep(1000);
                        System.out.print(xfer.getProgress()
                                .getBytesTransferred()
                                + "/"
                                + xfer.getProgress()
                                .getTotalBytesToTransfer()
                                + " bytes transferred of "+ file.getName() +" ....\n");
                    }
                    // loop with Transfer.isDone()
//                    xfer.addProgressListener(new UploadProgressLogger());
//                    XferMgrProgress.showTransferProgress(xfer);
                    //  or block with Transfer.waitForCompletion()
//                    xfer.waitForCompletion();
//                    xfer.getProgress();
                } catch (AmazonServiceException e) {
                    System.err.println(e.getErrorMessage());
//                    System.exit(1);
                    return new ResponseDTO<>().generateErrorResponse("Error In Connection" + e);
                }
            }

            xfer_mgr.shutdownNow();

//            try {
//                ObjectMetadataProvider metadataProvider = new ObjectMetadataProvider() {
//                    public void provideObjectMetadata(File file, ObjectMetadata metadata) {
//                        // If this file is a JPEG, then parse some additional info
//                        // from the EXIF metadata to store in the object metadata
////                        if (isJPEG(file)) {
////                            metadata.addUserMetadata("original-image-date",
////                                    parseExifImageDate(file));
////                        }
//                        metadata.addUserMetadata("original-image-date",
//                                "sad");
//                    }
//                };
//
//                MultipleFileUpload xfer = xfer_mgr.uploadFileList(input.getBucketName(),
//                        input.getDirectory(), new File("."), files, metadataProvider);
////                        .withCannedAcl(CannedAccessControlList.PublicRead));;
//                // loop with Transfer.isDone()
////                xfer_mgr.showTransferProgress();
//                // or block with Transfer.waitForCompletion()
//                xfer.waitForCompletion();
//            } catch (AmazonServiceException e) {
//                System.err.println(e.getErrorMessage());
//                System.exit(1);
//            }
//            xfer_mgr.shutdownNow();
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Erron S3");
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Object Uploaded Successfully");
    }


    public ResponseDTO uploadDirectory(UploadObjectsToBucketDTO input) {
        AmazonS3 client = cephConnection.setClientConnection();
        try {
            ArrayList<File> files = new ArrayList<File>();
            for (String path : input.getFilePathList()) {
                files.add(new File(path));
            }

            TransferManager xfer_mgr = TransferManagerBuilder.standard()
                    .withS3Client(client)
                    .withMinimumUploadPartSize((long)(5*1024*1024))
                    .withMultipartUploadThreshold((long)(5*1024*1024))
                    .build();

            try {
//                MultipleFileUpload xfer = xfer_mgr.uploadDirectory(input.getBucketName(),
//                        input.getDirectory(), new File(dir_path), recursive);
//                // loop with Transfer.isDone()
//                XferMgrProgress.showTransferProgress(xfer);
//                // or block with Transfer.waitForCompletion()
//                XferMgrProgress.waitForCompletion(xfer);
            } catch (AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);
            }
            xfer_mgr.shutdownNow();
        }  catch (AmazonServiceException e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Erron S3");
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseDTO<>().generateErrorResponse("Error In Connection");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "Object Uploaded Successfully");
    }
}
