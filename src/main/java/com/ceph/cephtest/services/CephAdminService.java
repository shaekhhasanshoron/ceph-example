package com.ceph.cephtest.services;

import com.ceph.cephtest.ResponseDTO;
import com.ceph.cephtest.dto.admin.CreateUserDTO;
import com.ceph.cephtest.dto.admin.UpdateUserDTO;
import com.ceph.cephtest.enums.ResponseStatus;
import com.ceph.cephtest.utils.AdminParameters;
import com.ceph.cephtest.utils.CephConnection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.impl.RgwAdminException;
import org.twonote.rgwadmin4j.model.BucketInfo;
import org.twonote.rgwadmin4j.model.BucketInfo.*;
import org.twonote.rgwadmin4j.model.BucketInfo.Usage.*;
import org.twonote.rgwadmin4j.model.Quota;
import org.twonote.rgwadmin4j.model.UsageInfo;
import org.twonote.rgwadmin4j.model.User;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class CephAdminService {

    @Autowired
    CephConnection cephConnection;

    @Autowired
    private ObjectMapper objectMapper;

    public ResponseDTO createUser(CreateUserDTO input) {
        try{
            RgwAdmin admin = cephConnection.setAdminConnection();
            if (input.getUserId() == null) {
                return new ResponseDTO<>().generateErrorResponse("User Id required");
            }
            Optional<User> existingUser = admin.getUserInfo(input.getUserId());
            if (existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User with same id already exists");
            }
            if (input.getMaxBuckets() <= 0) {
                return new ResponseDTO<>().generateErrorResponse("User maximum bucket should be greater than zero");
            }
            Map<String, String> parameters = new HashMap<>();
            parameters.put(AdminParameters.USER_MAX_BUCKETS,String.valueOf(input.getMaxBuckets()));
            parameters.put(AdminParameters.USER_CAPS, "buckets=read,write"); // By Default
            if (input.getUserName() != null && !input.getUserName().equals("")) {
                parameters.put(AdminParameters.USER_DISPLAY_NAME, input.getUserName());
            }
            if (input.getUserEmail() != null && !input.getUserEmail().equals("")) {
                parameters.put(AdminParameters.USER_EMAIL, input.getUserEmail());
            }

            User user = admin.createUser(input.getUserId(), parameters);
            admin.setUserQuota(user.getUserId(), input.getMaxObjects(), input.getMaxSize());
            return new ResponseDTO<>().generateSuccessResponse(user, "User has been created successfully");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO updateUser(UpdateUserDTO input) {
        try{
            RgwAdmin admin = cephConnection.setAdminConnection();
            if (input.getUserId() == null) {
                return new ResponseDTO<>().generateErrorResponse("User Id required");
            }
            Optional<User> user = admin.getUserInfo(input.getUserId());
            if (!user.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }

            if (input.getMaxBuckets() <= 0) {
                return new ResponseDTO<>().generateErrorResponse("User maximum bucket should be greater than zero");
            } else {
                List<String> listBucket= admin.listBucket(input.getUserId());
                if (input.getMaxBuckets() < listBucket.size()) {
                    return new ResponseDTO<>().generateErrorResponse(String.format("User already owns '%s' buckets, Max Bucket should be equal or greater than that.", listBucket.size()));
                }
            }
//            if (input.getMaxSize() <= 0) {
//                return new ResponseDTO<>().generateErrorResponse("User maximum use size should be greater than zero");
//            }
//            if (input.getMaxObjects() <= 0) {
//                return new ResponseDTO<>().generateErrorResponse("User maximum objects should be greater than zero");
//            }

            long numberOfObjectUsed = 0;
            long bucketUsedSizeInKB = 0;
            List<BucketInfo> bucketInfoList = admin.listBucketInfo(input.getUserId());
            for (BucketInfo bucketInfo: bucketInfoList) {
                Usage usage = bucketInfo.getUsage();
                if (usage.getRgwMain() != null) {
                    RgwMain rgwMain= usage.getRgwMain();
                    bucketUsedSizeInKB = bucketUsedSizeInKB + rgwMain.getSize_kb_actual();
                    numberOfObjectUsed = numberOfObjectUsed + rgwMain.getNum_objects();
                }
            }

            if (input.getMaxSize() > 0 && input.getMaxSize() <= bucketUsedSizeInKB) {
                return new ResponseDTO<>().generateErrorResponse(String.format("User already used '%s' KB space, Max size should be greater than that", bucketUsedSizeInKB));
            }

            if (input.getMaxObjects() > 0 && input.getMaxObjects() < numberOfObjectUsed) {
                return new ResponseDTO<>().generateErrorResponse(String.format("User already used '%s' objects, Max object limit should be greater than that", numberOfObjectUsed));
            }

            Map<String, String> parameters = new HashMap<>();
            if (input.getUserName() != null && !input.getUserName().equals("")) {
                parameters.put(AdminParameters.USER_DISPLAY_NAME, input.getUserName());
            }
            if (input.getUserEmail() != null && !input.getUserEmail().equals("")) {
                parameters.put(AdminParameters.USER_EMAIL, input.getUserEmail());
            }

            int userMaxBuckets = user.get().getMaxBuckets();
            if (input.getMaxBuckets() != userMaxBuckets) {
                parameters.put(AdminParameters.USER_MAX_BUCKETS, String.valueOf(input.getMaxBuckets()));
            }

            User modifyUser = admin.modifyUser(input.getUserId(), parameters);
            admin.setUserQuota(modifyUser.getUserId(), input.getMaxObjects(), input.getMaxSize());
            return new ResponseDTO<>().generateSuccessResponse(modifyUser, "User successfully updated");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }

    }


    public ResponseDTO getUserAvailableFreeSpace(String userId) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> user = admin.getUserInfo(userId);
            if (!user.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            ResponseDTO getUserQuotaResponse = getUserQuota(userId);
            if (getUserQuotaResponse.getStatus() == ResponseStatus.error) {
                return new ResponseDTO<>().generateErrorResponse(getUserQuotaResponse.getMessage());
            }

            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            Quota userQuota = objectMapper.convertValue(getUserQuotaResponse.getData(), new TypeReference<Quota>() { });
            long userOwnedSizeInKB = userQuota.getMaxSizeKb();
            if (userOwnedSizeInKB < 0) {
                return new ResponseDTO<>().generateSuccessResponse(Long.MAX_VALUE, "User Usage response");
            }

            long bucketUsedSizeInKB = 0;
            List<BucketInfo> bucketInfoList = admin.listBucketInfo(userId);
            for (BucketInfo bucketInfo: bucketInfoList) {
                Usage usage = bucketInfo.getUsage();
                if (usage.getRgwMain() != null) {
                    RgwMain rgwMain= usage.getRgwMain();
                    bucketUsedSizeInKB = bucketUsedSizeInKB + rgwMain.getSize_kb_actual();
                }
            }
            return new ResponseDTO<>().generateSuccessResponse(userOwnedSizeInKB - bucketUsedSizeInKB, "User Usage response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO getUserUsage(String userId) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> existingUser = admin.getUserInfo(userId);
            if (!existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            Optional<UsageInfo> userUsage = admin.getUserUsage(userId);
            if (!userUsage.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("No usage found for user");
            }
            return new ResponseDTO<>().generateSuccessResponse(userUsage, "User Usage response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO getUserQuota(String userId) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> existingUser = admin.getUserInfo(userId);
            if (!existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            Optional<Quota> quota = admin.getUserQuota(userId);
            if (!quota.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("No Quota found for user");
            }
            return new ResponseDTO<>().generateSuccessResponse(quota.get(), "User Quota response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }


    public ResponseDTO deleteUser(String userId) {
        try{
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> existingUser = admin.getUserInfo(userId);
            if (!existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            admin.removeUser(userId);
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
        return new ResponseDTO<>().generateSuccessResponse(null, "User successfully deleted");
    }

    public ResponseDTO getUser(String userId) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> existingUser = admin.getUserInfo(userId);
            if (!existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            return new ResponseDTO<>().generateSuccessResponse(existingUser, "User response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }


    public ResponseDTO getUserList() {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            List<User> userList = admin.listUserInfo();
            return new ResponseDTO<>().generateSuccessResponse(userList, "User list response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO getBucketInfo(String bucketName) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<BucketInfo> getBucketInfo = admin.getBucketInfo(bucketName);
            if (!getBucketInfo.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("Bucket Not Found");
            }
            return new ResponseDTO<>().generateSuccessResponse(getBucketInfo, "Bucket info response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO getBucketInfoListForUser(String userId) {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            Optional<User> existingUser = admin.getUserInfo(userId);
            if (!existingUser.isPresent()) {
                return new ResponseDTO<>().generateErrorResponse("User not found");
            }
            List<BucketInfo> bucketInfoList = admin.listBucketInfo(userId);
            return new ResponseDTO<>().generateSuccessResponse(bucketInfoList, "Bucket list response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    public ResponseDTO getAllBucketInfo() {
        try {
            RgwAdmin admin = cephConnection.setAdminConnection();
            List<BucketInfo> bucketInfoList = admin.listBucketInfo();
            return new ResponseDTO<>().generateSuccessResponse(bucketInfoList, "Bucket list response");
        } catch (RgwAdminException e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse(getRgwAdminErrorMessage(e));
        } catch (NullPointerException e) {
            return new ResponseDTO<>().generateErrorResponse("Invalid URL");
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseDTO<>().generateErrorResponse("Something wrong in connecting");
        }
    }

    private String getRgwAdminErrorMessage(RgwAdminException e) {
        if (e.getMessage().equalsIgnoreCase("SignatureDoesNotMatch")) {
            return "Incorrect access and secret key";
        } else if (e.getMessage().equalsIgnoreCase("InvalidAccessKeyId")) {
            return "Invalid Access Key";
        } else {
            return "Invalid Connection";
        }
    }
}
