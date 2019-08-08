package com.xz.sink;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xz.pojo.S3WriterBean;
import com.xz.utils.DateUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.RollingFileSink;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kouyouyang on 2019-07-20 18:09
 */
public class S3Sink extends AbstractSink implements Configurable, BatchSizeSupported {

    // LOGGING
    private static final Logger logger = LoggerFactory
            .getLogger(RollingFileSink.class);

    // 默认参数
    private static final long defaultRollInterval = 300;
    private static final int defaultBatchSize = 500;
    private String defaultAvroSchema = "";
    private String defaultAvroSchemaRegistryURL = "";
    private String defaultFilePrefix = "flumeS3Sink";
    private String defaultFileSufix = ".data";
    private String defaultTempFile = "/tmp/flumes3sink/data/file";
    private String defaultCompress = "false";

    // SINK CONF PARAMETERS
    private int batchSize = defaultBatchSize;
    private long rollInterval = defaultRollInterval;
    private String avroSchema = defaultAvroSchema;
    private String avroSchemaRegistryURL = defaultAvroSchemaRegistryURL;
    private String filePrefix = defaultFilePrefix;
    private String fileSufix = defaultFileSufix;
    private String bucketName;
    private String awsRegion;
    private String tempFile = defaultTempFile;
    private String compress = defaultCompress;

    // SINK COUNTER
    private SinkCounter sinkCounter;

    private Boolean isTimeDeadlineRotate=false;
    private Boolean shouldRotate;
    private ScheduledExecutorService rollService;

    //STREAMS
    private DataFileWriter<GenericRecord> dataFileWriter = null;

    // AWS
    private S3Writer as3w;

    // SINK COUNTER
    private long sink_current_counter = 0;

    //DERSERIALIZE CONSTANTS
    private static final byte MAGIC_BYTE = 0x0;
    private static final int idSize = 4;

    //FILE OUTPUT STREAM
    private FileOutputStream fileOutputStream = null;
    private S3WriterBean s3WriterBean=null;

    // Start transaction
    private Status status = null;
    private Channel ch = null;
    private Transaction txn = null;

    // SCHEMA
    private String schemaStr = null;
    private Schema schema = null;

    // WRITER
    private DatumWriter<GenericRecord> datumWriter = null;
    //临时文件大小
    private long tempFileSize;
    private File currentTempFile;

    public S3Sink() {
        shouldRotate = false;
    }

    public void configure(Context context) {
        int batchSize = context.getInteger("s3.batchSize", defaultBatchSize);
        long rollInterval = context.getLong("s3.rollInterval", defaultRollInterval);
        String avroSchema = context.getString("s3.AvroSchema", defaultAvroSchema);
        String avroSchemaRegistryURL = context.getString("s3.AvroSchemaRegistryURL", defaultAvroSchemaRegistryURL);
        String filePrefix = context.getString("s3.filePrefix", defaultFilePrefix);
        String fileSufix = context.getString("s3.FileSufix", defaultFileSufix);
        String bucketName = context.getString("s3.bucketName");
        String awsRegion = context.getString("s3.awsRegion");
        String tempFile = context.getString("s3.tempFile", defaultTempFile);
        String compress = context.getString("s3.compress", defaultCompress);

        Preconditions.checkArgument(bucketName != null, "[INFO] BUCKET NAME IS NOT SPECIFIED (example :  s3.bucket = \"aws_bucket_name\")");
        Preconditions.checkArgument(awsRegion != null, "[INFO] AWS REGION IS NOT SPECIFIED (example :  s3.awsRegion = \"eu-central-1\")");

        this.batchSize = batchSize;
        this.rollInterval = rollInterval;
        this.avroSchema = avroSchema;
        this.avroSchemaRegistryURL = avroSchemaRegistryURL;
        this.filePrefix = filePrefix;
        this.fileSufix = fileSufix;
        this.bucketName = bucketName;
        this.awsRegion = awsRegion;
        this.sink_current_counter = batchSize;
        this.tempFile = tempFile;
        this.compress = compress;
        this.s3WriterBean=new S3WriterBean();

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public void start() {
        double randomDouble = Math.random();
        randomDouble = randomDouble * 1500 + 1;
        int randomInt = (int) randomDouble;
        this.tempFile = this.tempFile + "-" + randomInt + "." + this.fileSufix;
        currentTempFile = new File(tempFile);
        //初始化aws连接
        as3w = new S3Writer(this.awsRegion);
        // STARTING THE SINK COUNTER
        sinkCounter.start();
        super.start();
        if (rollInterval > 0) {
            rollService = Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder().setNameFormat(
                            "rollingFileSink-roller-" +
                                    Thread.currentThread().getId() + "-%d").build());
            rollService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    shouldRotate = true;
                }// 从现在开始rollInterval秒钟之后，每隔rollInterval秒钟执行一次job
            }, 0, rollInterval, TimeUnit.SECONDS);
        } else {
            logger.info("滚动间隔无效，文件滚动不会发生.");
        }
        // GET THE SCHEMA
        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
            if (this.avroSchema != null && !this.avroSchema.isEmpty()) {
                schemaStr = getSchemaFromPath(this.avroSchema);
            } else if (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty()) {
                schemaStr = getSchemaFromURL(this.avroSchemaRegistryURL);
            }
            schema = getAvroSchema(schemaStr);
        }
        logger.info("FLUME S3 SINK {} started...", getName());
    }

    @Override
    public void stop() {
        super.stop();
        as3w.closeClient();
        logger.info("s3Client shutdown");
    }

    public Status process() throws EventDeliveryException {
        //获取序列化方式
        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
            if (this.avroSchema != null && !this.avroSchema.isEmpty()) {
                schemaStr = getSchemaFromPath(this.avroSchema);
            } else if (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty()) {
                schemaStr = getSchemaFromURL(this.avroSchemaRegistryURL);
            }
            Schema current_schema = getAvroSchema(schemaStr);
            if (!current_schema.toString().equalsIgnoreCase(schema.toString())){
                logger.info("[INFO] SCHEMA IS CHANGED. HENCE FILE IS ROTATING...");
                shouldRotate = true;
            }
            schema = current_schema;
        }
        if (shouldRotate || sink_current_counter == this.batchSize) {
            logger.info("开始滚动 {} 文件入 S3", this.tempFile);
            //只有最开始执行一次
            if (!currentTempFile.exists()) {
                try {
                    currentTempFile.createNewFile();
                    fileOutputStream = new FileOutputStream(currentTempFile);
                    if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                        // GET WRITER
                        datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                        dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                        if(this.compress.equals("true")) {
                            dataFileWriter.setCodec(CodecFactory.snappyCodec());
                        }
                        dataFileWriter.create(schema, fileOutputStream);
                        tempFileSize = currentTempFile.length();
                    }
                    sink_current_counter = 0;
                    shouldRotate = false;
                    ch = getChannel();
                    txn = ch.getTransaction();
                    txn.begin();
                } catch (IOException e) {
                    logger.info("CANNOT CREATE TEMP FILE TO PARTIALLY STORE AWS DATA {}:", this.tempFile);
                    e.printStackTrace();
                    throw new EventDeliveryException("Failed to process transaction", e);
                }
            } else {
                // 检查文件是否为空，如果为空，设置非滚动模式
                if (currentTempFile.length() == 0 || currentTempFile.length() == tempFileSize) {
                    logger.info("文件为空所以没有数据写入 S3 {}:", this.tempFile);
                    sink_current_counter = 0;
                    shouldRotate = false;
                }
                else {
                    // AWS写数据
                    try {
                        if(as3w.putFile(this.bucketName, this.filePrefix, this.fileSufix, this.tempFile,this.s3WriterBean)){
                            currentTempFile.delete();
                            currentTempFile = new File(tempFile);
                            currentTempFile.createNewFile();
                            fileOutputStream = new FileOutputStream(currentTempFile);
                        }
                        // GET WRITER
                        if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                            datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                            if(this.compress.equals("true")) {
                                dataFileWriter.setCodec(CodecFactory.snappyCodec());
                            }
                            dataFileWriter.create(schema, fileOutputStream);
                        }
                        sink_current_counter = 0;
                        shouldRotate = false;
                        logger.info("临时文件 {} 写入 S3 {} SUCCESSFUL", this.tempFile, this.bucketName);
                        txn.commit();
                    } catch (Exception e) {
                        txn.rollback();
                        status = Status.BACKOFF;
                        logger.info("WRITING DATA FORM TEMP FILE {} TO S3 {} FAILED",this.tempFile,this.bucketName);
                        e.printStackTrace();
                        throw new EventDeliveryException("Failed to process transaction", e);
                    } finally {
                        txn.close();
                        txn = null;
                    }
                }

                if (txn ==  null){
                    ch = getChannel();
                    txn = ch.getTransaction();
                    txn.begin();
                }
            }

        }
        // WRITE THE DATA TEMP FILE
        try {
            Event event = ch.take();
            if (event != null) {
                byte[] serializeBytes = event.getBody();
                Map<String, String> eventHeaders = event.getHeaders();
                if ((this.avroSchema != null && !this.avroSchema.isEmpty()) || (this.avroSchemaRegistryURL != null && !this.avroSchemaRegistryURL.isEmpty())) {
                    // AVRO BINARY FORMAT WITHOUT SCHEMA
                    ByteArrayDeserializer bad = new ByteArrayDeserializer();
                    byte[] deserializeBytes = bad.deserialize(eventHeaders.get("topic"), serializeBytes);
                    ByteBuffer buffer = getByteBuffer(deserializeBytes);
                    int id = buffer.getInt();
                    int length = buffer.limit() - 1 - idSize;
                    final Object result;
                    int start = buffer.position() + buffer.arrayOffset();
                    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                    final DecoderFactory decoderFactory = DecoderFactory.get();
                    GenericRecord userData = null;
                    userData = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));
                    dataFileWriter.append(userData);
                    dataFileWriter.flush();
                    sinkCounter.incrementEventDrainSuccessCount();
                    long success_count = sinkCounter.getEventDrainSuccessCount();
                    if(success_count <= batchSize)
                        sink_current_counter = success_count;
                    else
                        sink_current_counter++;
                } else {
                    //不序列化方式获取消息
                    if(eventHeaders!=null){
                        String timestamp ="";
                        if (eventHeaders.containsKey("timestamp")) {
                            timestamp=DateUtil.stampToBeijingDate(eventHeaders.get("timestamp"), 8);
                        }else{
                            timestamp=DateUtil.stampToBeijingDate(Long.toString(System.currentTimeMillis()),8);
                        }
                        s3WriterBean.setDay(DateUtil.getDayFromTimeStamp(timestamp));
                        s3WriterBean.setHour(DateUtil.getHourFromTimeStamp(timestamp));
                        s3WriterBean.setMinute(DateUtil.getMinuteFromTimeStamp(timestamp));
                        s3WriterBean.setSecond(DateUtil.getSecondFromTimeStamp(timestamp));
                        s3WriterBean.setMillions(DateUtil.getMillionsFromTimeStamp(timestamp));
                    }
                    s3WriterBean.setServerAddress(InetAddress.getLocalHost().getHostAddress());
//                    logger.info("事件体----------"+eventHeaders);
                    //字节数组转为字符串
                    String str = new String(serializeBytes)+"\n";
                    byte b[] = str.getBytes();
                    fileOutputStream.write(b);
                    s3WriterBean.setFileOutputStream(fileOutputStream);
                    sinkCounter.incrementEventDrainSuccessCount();
                    long success_count = sinkCounter.getEventDrainSuccessCount();
                    //当时间戳为每小时最后一秒最后10毫秒的时候
                    //设置一个标注说明每小时最后时刻滚动过没有，如果为true表示滚动过，接下来的消息进来就不再滚动，为false表示没滚动过，满足条件滚动
                    if(!("59").equals(s3WriterBean.getMinute())&&!("59").equals(s3WriterBean.getSecond())&&!s3WriterBean.getMillions().startsWith("9")){
                        isTimeDeadlineRotate=false;
                    }
                    if(("59").equals(s3WriterBean.getMinute())&&("59").equals(s3WriterBean.getSecond())&&s3WriterBean.getMillions().startsWith("9")&&!isTimeDeadlineRotate){
                        shouldRotate=true;
                        isTimeDeadlineRotate=true;
                    }
                    if(success_count <= batchSize)
                        sink_current_counter = success_count;
                    else
                        sink_current_counter++;
                }
            }
            status = Status.READY;
        } catch (Exception e) {
            txn.rollback();
            e.printStackTrace();
            status = Status.BACKOFF;
            logger.info("FAILED TO WRITE DATA TO TEMP FILE {}", this.tempFile);
            throw new EventDeliveryException("Failed to process transaction", e);
        }
        return status;
    }


    public String getSchemaFromURL(String schemaRegistryURL) {
        try {
            URL schemaData = new URL(schemaRegistryURL);
            String schemaJsonData = Resources.toString(schemaData, Charsets.UTF_8).replace("\\\"", "\"")
                    .replace("\"{", "{").replace("}\"", "}");
            JSONObject obj = new JSONObject(schemaJsonData);
            String data = obj.getJSONObject("schema")
                    .toString() ;
//                    .replace("\"string\"", "{\"type\": \"string\",\"avro.java.string\": \"String\"}");

            return data;

        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return "";
    }

    public String getSchemaFromPath(String schemaPath) {

        try {

            Path file_path = Paths.get(schemaPath);
            String data = new String(Files.readAllBytes(file_path), "UTF-8");

            return data;
        } catch (UnsupportedEncodingException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return "";
    }

    public Schema getAvroSchema(String data) {

        if (data != null && !data.isEmpty()) {
            try {
                Schema schema = new Schema.Parser().parse(data);

                return schema;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return null;
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

}