package com.alibaba.otter.canal.protocol;

import com.alibaba.otter.canal.protocol.canal.KafkaMessageSerializer;
import com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.entity.EntryType;
import com.alibaba.otter.canal.protocol.entity.Header;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.flink.pb3.canal.entity.CanalPacket.PacketType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class KafkaProducerTest {

  private Logger logger = LoggerFactory.getLogger(KafkaProducerTest.class);
  private Properties properties = new Properties();


  public static final String  topic     = "canal10";
  public static final Integer partition = null;
  public static final String  groupId   = "canal-format";
  public static final String  servers   = "yuluo-10:9092,yuluo-11:9092,yuluo-12:9092";
  public static final String  zkServers = "yuluo-10:2181,yuluo-11:2181,yuluo-12:2181";

  @Before
  public void setUp(){
    properties = new Properties();
    properties.put("bootstrap.servers", servers);
    properties.put("key.serializer", StringSerializer.class);
    properties.put("value.serializer", KafkaMessageSerializer.class);
    properties.put("enable.auto.commit", false);
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("auto.offset.reset", "latest"); // 如果没有offset则从最后的offset开始读
    properties.put("request.timeout.ms", "40000"); // 必须大于session.timeout.ms的设置
    properties.put("session.timeout.ms", "30000"); // 默认为30秒
    properties.put("isolation.level", "read_committed");
  }

  @Test
  public void testKafkaProducer(){
    KafkaProducer<String, Message> producer = new KafkaProducer<String, Message>(properties);


    int batchSize =200000;//同步的binlog数量
    int count = 20000;
    while(count<batchSize){


      //TODO 封装CanalPacket数据包

      Header.Builder headerBuilder = Header.newBuilder();
      headerBuilder.setLogFileName("mysql-bin.00000"+String.valueOf(count));
      headerBuilder.setLogFileOffset(1024);
      headerBuilder.setExecuteTime(1024);
      //组装消费位点
      String notice = "I'am recevied message,Current Message ID:"+count;
      ByteString item = ByteString.copyFrom(notice.getBytes(StandardCharsets.UTF_8));
      Entry.Builder entryBuilder = Entry.newBuilder();
      entryBuilder.setHeader(headerBuilder.build());
      entryBuilder.setEntryType(EntryType.ROWDATA);
      entryBuilder.setStoreValue(item);
      Entry entry = entryBuilder.build();
      List<Entry> list = new ArrayList<>();
      list.add(entry);
      Message message = new Message(count,true, Arrays.asList(entry.toByteString()));
      message.setEntries(list);

      List<ByteString> rawEntries = new ArrayList<>();


      try {

        ProducerRecord<String,Message> record =
            new ProducerRecord<String,Message>(topic,"test-message-"+String.valueOf(count),message);
        RecordMetadata recordMetadata = producer.send(record).get();
        logger.info("写入Canal消息到Kafka，当前消息为: {},发送成功后的响应为: {} ",
            message,
            recordMetadata);
      }catch (Exception ex){
        ex.printStackTrace();
      }

      ++count;
    }



  }

  @SuppressWarnings("deprecation")
  private byte[] buildData(Message message) throws IOException {
    List<ByteString> rowEntries = message.getRawEntries();
    // message size
    int messageSize = 0;
    messageSize += CodedOutputStream.computeInt64Size(1, message.getId());

    int dataSize = 0;
    for (ByteString rowEntry : rowEntries) {
      dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
    }
    messageSize += dataSize;
    messageSize += 1 * rowEntries.size();
    // packet size
    int size = 0;
    size += CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber());
    size += CodedOutputStream.computeTagSize(5)
        + CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize;
    // TODO recyle bytes[]
    byte[] body = new byte[size];
    CodedOutputStream output = CodedOutputStream.newInstance(body);
    output.writeEnum(3, PacketType.MESSAGES.getNumber());

    output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    output.writeRawVarint32(messageSize);
    // message
    output.writeInt64(1, message.getId());
    for (ByteString rowEntry : rowEntries) {
      output.writeBytes(2, rowEntry);
    }
    output.checkNoSpaceLeft();

    return body;
  }

}
