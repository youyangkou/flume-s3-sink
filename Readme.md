# FLUME SINK FOR AWS S3

## INTRODUCTION
该项目为flume的S3 sink插件，运行该项目需要AWS通行证credentials (Credentials stored in ~/.aws/credentails file)

该自定义S3 sink支持滚动条件如下：
1. Batch Size
2. Roll Interval
3. Compression

写入S3时，可以自动安装天和时间分区，格式如下
For example, the file path will look like this 
```
s3://customers/dt=2019-08-01/hr=22/log-SEH-NHT-1.txt
```

##### 基本架构

![picture](img/flumes3sink.png)

##### 该sink的默认参数
```
   defaultRollInterval           = 300;
   defaultBatchSize              = 500;
   defaultAvroSchema             = "";
   defaultAvroSchemaRegistryURL  = "";
   defaultFilePrefix             = "flumeS3Sink";
   defaultFileSufix              = ".data";
   defaultTempFile               = "/tmp/flumes3sink/data/file";
   defaultCompress               = "false";
```
## 如何构建该sink
1. Clone the git repo
2. Run the maven command
```
mvn package 
```

###### Reference : <i>https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html </i>

## 如何配置该插件
在Flume安装目录下创建plugins.d/flumes3sink/lib文件夹，将打包好的s3 sink上传到该文件夹下

# FLUME SINK CONF
#注意事项：
# don't give / at the end for the bucketname
# roll interval 以为秒为单位
# compression codec is snappy
# 如果无序列化文件tracking.avsc可忽视a1.sinks.k1.s3.AvroSchema该参数

```
a1.sinks.k1.type = com.rab4u.flume.FlumeS3Sink
a1.sinks.k1.s3.bucketName = dp-cts-stream-test/after-sales/tracking
a1.sinks.k1.s3.awsRegion = eu-central-1
a1.sinks.k1.s3.filePrefix = after-sales
a1.sinks.k1.s3.FileSufix = avro
a1.sinks.k1.s3.rollInterval = 60
a1.sinks.k1.s3.tempFile = /home/ravindrachellubani/Documents/code/git/apache-flume-to-s3/after-sales-temp-file
a1.sinks.k1.s3.AvroSchema = /home/ravindrachellubani/Documents/code/git/apache-flume-to-s3/tracking.avsc
a1.sinks.k1.s3.batchSize = 500
a1.sinks.k1.s3.compress = false
```
## aws配置
AWS Credentials Not Found

check the ~/.aws/credentials file is present or not. if not create by running the following command
```
aws configure
```
## REFERENCES
1. https://flume.apache.org/FlumeDeveloperGuide.html
2. https://github.com/apache/flume
3. https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/uploadobjusingmpu.html