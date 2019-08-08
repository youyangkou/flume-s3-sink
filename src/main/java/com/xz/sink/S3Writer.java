package com.xz.sink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.xz.pojo.S3WriterBean;
import java.io.File;
import java.util.UUID;

/**
 * Created by kouyouyang on 2019-07-20 18:09
 */
public class S3Writer {

    private AmazonS3 s3Client;
    private TransferManager tm;

    S3Writer(String clientRegion) {
        try {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();
            tm = TransferManagerBuilder.standard()
                    .withS3Client(s3Client)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public Boolean putFile(String bucketName, String filePrefix, String fileSufix, String file, S3WriterBean s3WriterBean) throws InterruptedException {
        Boolean flag=false;
        Upload upload=null;
        long l1 = System.currentTimeMillis();
        try{
            NoArgGenerator timeBasedGenerator = Generators.timeBasedGenerator();
            UUID firstUUID = timeBasedGenerator.generate();
            //分区天和小时
            String dateString = s3WriterBean.getDay();
            String hours = s3WriterBean.getHour();
            String serverAddress = s3WriterBean.getServerAddress().replace(".", "-");
            // 异步传输
            upload = tm.upload(bucketName + "/dt=" + dateString + "/hr=" + hours, filePrefix + "-" + firstUUID.toString()+"-"+serverAddress + "." + fileSufix, new File(file));
            // 在继续之前等待上传完成
            upload.waitForCompletion();
            if(upload.isDone()){
                System.out.println("本批消息最大时间"+ s3WriterBean.getDay()+" "+s3WriterBean.getHour()+":"+ s3WriterBean.getMinute()+":"+s3WriterBean.getSecond()+"\n"
                        +"[INFO] 成功写入S3 PATH : S3://" + bucketName + "/dt=" + dateString + "/hr=" + hours + "/" + filePrefix + "-" + firstUUID.toString()+"-"+serverAddress + "." + fileSufix);
                flag=true;
            }else{
                flag=false;
            }
        }
        catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        if(flag){
            long l2 = System.currentTimeMillis();
            System.out.println("上传花费"+(l2-l1)/1000+"s");
        }else{
            System.out.println("上传失败");
        }
        return flag;
    }


    public void closeClient() {
        tm.shutdownNow();
        s3Client.shutdown();
    }

}
