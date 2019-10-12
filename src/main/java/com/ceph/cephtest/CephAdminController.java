package com.ceph.cephtest;

import com.ceph.cephtest.dto.admin.CreateUserDTO;
import com.ceph.cephtest.dto.admin.UpdateUserDTO;
import com.ceph.cephtest.services.CephAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.twonote.rgwadmin4j.RgwAdmin;
import org.twonote.rgwadmin4j.RgwAdminBuilder;
import org.twonote.rgwadmin4j.model.Quota;
import org.twonote.rgwadmin4j.model.S3Credential;
import org.twonote.rgwadmin4j.model.User;

import javax.validation.Valid;
import java.util.*;

@RestController
@RequestMapping("/ceph-admin")
public class CephAdminController {

    @Autowired
    CephAdminService cephAdminService;

    @RequestMapping(value = "/create-user", method = RequestMethod.POST)
    public ResponseEntity<?> createUser(@Valid @RequestBody CreateUserDTO input) {
        ResponseDTO result = cephAdminService.createUser(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/update-user", method = RequestMethod.POST)
    public ResponseEntity<?> updateUser(@Valid @RequestBody UpdateUserDTO input) {
        ResponseDTO result = cephAdminService.updateUser(input);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/delete-user/{user-id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteUser(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.deleteUser(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-user/{user-id}", method = RequestMethod.GET)
    public ResponseEntity<?> getUserInfo(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.getUser(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-user-list", method = RequestMethod.GET)
    public ResponseEntity<?> getUserList() {
        ResponseDTO result = cephAdminService.getUserList();
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-user-usage/{user-id}", method = RequestMethod.GET)
    public ResponseEntity<?> getUserUsage(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.getUserUsage(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-user-quota/{user-id}", method = RequestMethod.GET)
    public ResponseEntity<?> getUserQuota(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.getUserQuota(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    //It will return result in kb
    @RequestMapping(value = "/get-user-available-free-space/{user-id}", method = RequestMethod.GET)
    public ResponseEntity<?> getUserAvailableFreeSpace(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.getUserAvailableFreeSpace(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/get-user-bucket-info/{user-id}", method = RequestMethod.GET)
    public ResponseEntity<?> getBucketInfoListForUser(@PathVariable("user-id") String userId) {
        ResponseDTO result = cephAdminService.getBucketInfoListForUser(userId);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }




    @RequestMapping(value = "/create-user-draft", method = RequestMethod.POST)
    public ResponseEntity<?> createUserDraft(@Valid @RequestBody CreateUserDTO input) {
        try{
            RgwAdmin RGW_ADMIN = new RgwAdminBuilder()
                    .accessKey("shoron-access-key")
                    .secretKey("shoron-secret-key")
                    .endpoint("http://rook-ceph-ext-obj-store.devxn.com:30010/admin")
                    .build();
            Optional<User> existingUser = RGW_ADMIN.getUserInfo(input.getUserId());
            if (existingUser.isPresent()) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("User already exists"), HttpStatus.OK);
            }
            Map<String, String> parameters = new HashMap<>();
            parameters.put("display-name", "cloudslip");
            /*
                caps.type > buckets, usage, users, metadata, zone
                caps.perm > read, write
             */
//            parameters.put("user-caps", "usage=read");
            parameters.put("max-buckets", "1");
            User user = RGW_ADMIN.createUser(input.getUserId(), parameters);
            System.out.println(user);
            return new ResponseEntity<>(new ResponseDTO<>().generateSuccessResponse(user, "User Successfully Created"), HttpStatus.OK);
        } catch (org.twonote.rgwadmin4j.impl.RgwAdminException e) {
            System.out.println(e);
            if (e.getMessage().equalsIgnoreCase("InvalidAccessKeyId") || e.getMessage().equalsIgnoreCase("SignatureDoesNotMatch")) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid Access/Secret key for user"), HttpStatus.OK);
            }
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (NullPointerException e) {
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Something wrong in connecting"), HttpStatus.OK);
        }
    }


    @RequestMapping(value = "/update-user-draft", method = RequestMethod.POST)
    public ResponseEntity<?> updateUserDraft(@Valid @RequestBody CreateUserDTO input) {
        try{
            RgwAdmin RGW_ADMIN = new RgwAdminBuilder()
                    .accessKey("shoron-access-key")
                    .secretKey("shoron-secret-key")
                    .endpoint("http://rook-ceph-ext-obj-store.devxn.com:30010/admin")
                    .build();
            Optional<User> existingUser = RGW_ADMIN.getUserInfo(input.getUserId());
            if (!existingUser.isPresent()) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("User do not exists"), HttpStatus.OK);
            }
            Map<String, String> parameters = new HashMap<>();
            parameters.put("display-name", "Shaekh Hasan Shoron");
            parameters.put("user-caps", "buckets=read,write;usage=read");
            parameters.put("max-buckets", "3");
            User user = RGW_ADMIN.modifyUser(input.getUserId(), parameters);
            RGW_ADMIN.setUserQuota(input.getUserId(), 500, 10000);
            Optional<Quota> quota = RGW_ADMIN.getUserQuota(input.getUserId());
            System.out.println(user);
            return new ResponseEntity<>(new ResponseDTO<>().generateSuccessResponse(user, "User Successfully Updated"), HttpStatus.OK);
        } catch (org.twonote.rgwadmin4j.impl.RgwAdminException e) {
            System.out.println(e);
            if (e.getMessage().equalsIgnoreCase("InvalidAccessKeyId") || e.getMessage().equalsIgnoreCase("SignatureDoesNotMatch")) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid Access/Secret key for user"), HttpStatus.OK);
            }
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (NullPointerException e) {
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Something wrong in connecting"), HttpStatus.OK);
        }
    }

    @RequestMapping(value = "/create-s3-credential-draft", method = RequestMethod.POST)
    public ResponseEntity<?> createUserS3Credentials(@Valid @RequestBody CreateUserDTO input) {
        try{
            RgwAdmin RGW_ADMIN = new RgwAdminBuilder()
                    .accessKey("shoron-access-key")
                    .secretKey("shoron-secret-key")
                    .endpoint("http://rook-ceph-ext-obj-store.devxn.com:30010/admin")
                    .build();
            Optional<User> existingUser = RGW_ADMIN.getUserInfo(input.getUserId());
            if (!existingUser.isPresent()) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("User do not exists"), HttpStatus.OK);
            }
            List<S3Credential> s3Credentials = RGW_ADMIN.createS3Credential(input.getUserId());
            System.out.println(s3Credentials);
            return new ResponseEntity<>(new ResponseDTO<>().generateSuccessResponse(s3Credentials, "User S3 Credentials Successfully Created"), HttpStatus.OK);
        } catch (org.twonote.rgwadmin4j.impl.RgwAdminException e) {
            System.out.println(e);
            if (e.getMessage().equalsIgnoreCase("InvalidAccessKeyId") || e.getMessage().equalsIgnoreCase("SignatureDoesNotMatch")) {
                return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid Access/Secret key for user"), HttpStatus.OK);
            }
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (NullPointerException e) {
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Invalid URL"), HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e);
            return new ResponseEntity<>(new ResponseDTO<>().generateErrorResponse("Something wrong in connecting"), HttpStatus.OK);
        }
    }
}
