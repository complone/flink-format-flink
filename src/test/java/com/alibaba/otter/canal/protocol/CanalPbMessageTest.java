package com.alibaba.otter.canal.protocol;

import com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.entity.EntryType;
import com.alibaba.otter.canal.protocol.entity.Header;
import com.alibaba.otter.canal.protocol.entity.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.pb.PbRowDeserializationSchema;
import org.apache.flink.pb.PbRowSerializationSchema;
import org.apache.flink.pb.PbRowTypeInformation;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalPbMessageTest {

  private static Logger logger = LoggerFactory.getLogger(CanalPbMessageTest.class);
  @Test
  public void testCanalPbMessage(){
    int batchSize =200000;//同步的binlog数量
    int count = 20000;
    RowType rowType = PbRowTypeInformation.generateRowType(Message.getDescriptor());

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
      Message message = Message.newBuilder()
          .addAllEntries(list)
          .setId(count)
          .setRaw(true)
          .addAllRawEntries(Arrays.asList(entry.toByteString()))
          .build();
      //收集RowData数据
    Collector<RowData> out = new ListOutputCollector();
    //通用protobuf 反序列化
    String[] importStringName = new String[] {"com.google.protobuf.ByteString","com.alibaba.otter.canal.protocol.entity.Message"};
    PbRowDeserializationSchema pbRowDeserializationSchema = new PbRowDeserializationSchema(rowType,
          "com.alibaba.otter.canal.protocol.entity.Message",importStringName,
          true,true);

    //canal序列化 message解析
//    CanalRowDeserializationSchema canalRowDeserializationSchema = new CanalRowDeserializationSchema(rowType,
//        "com.alibaba.otter.canal.protocol.entity.Message",
//        false,TimestampFormat.SQL);


        try {
          RowData result = pbRowDeserializationSchema.deserialize(message.toByteArray());
          logger.info("当前结果为: {}",result);
//          Descriptors.Descriptor descriptor = Header.getDescriptor();


          RowType rowType1 = PbRowTypeInformation.generateRowType(
              org.apache.flink.pb3.canal.entity.Message.getDescriptor());
//          PbRowSerializationSchema pbRowSerializationSchema = new PbRowSerializationSchema(rowType1,"com.alibaba.otter.canal.protocol.entity.Message");
//          byte[] resul1 = pbRowSerializationSchema.serialize(result);
//          logger.info("当前结果为: {}",resul1);
//      pbRowDeserializationSchema.deserialize(message.toByteArray(),out);
//      pbRowDeserializationSchema.deserialize(message.toByteArray());
        }catch (Exception exception){
          exception.printStackTrace();
        }
  }

    private static final class ListOutputCollector implements Collector<RowData>{

      private final List<RowData> output = new ArrayList<>();

      @Override
      public void collect(RowData row) {
        this.output.add(row);
      }

      @Override
      public void close() {

      }

      public List<RowData> getOutput() {
        return output;
      }
    }

}
