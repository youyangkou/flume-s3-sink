package com.xz.utils;



import java.io.File;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
/**
 * 高级别上传S3文件
 * @author kouyouyang
 * @date 2019-07-29 22:57
 */
public class HighLevelMultipartUpload {

    public static void main(String[] args) throws Exception {
        String clientRegion = "cn-north-1";
//        String bucketName = args[0];
        String bucketName = "xz-emr-hive-data/hive/algorithm/nginx_log";
        String keyName = "nginx_log.txt";
//        String keyName = args[1];
//        String filePath = args[2];
        String filePath = "/Users/kouyouyang/Desktop/log";
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
            TransferManager tm = TransferManagerBuilder.standard()
                    .withS3Client(s3Client)
                    .build();
            // 异步传输
            Upload upload = tm.upload(bucketName, keyName, new File(filePath));
            System.out.println("Object upload started");
            long l1 = System.currentTimeMillis();
            // Optionally, wait for the upload to finish before continuing.
            upload.waitForCompletion();
            System.out.println("Object upload complete");
            long l2 = System.currentTimeMillis();
            System.out.println("上传花费"+(l2-l1)/1000+"s");
        }
        catch(AmazonServiceException e) {
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            e.printStackTrace();
        }
    }
}



