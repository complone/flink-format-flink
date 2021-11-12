package com.alibaba.otter.canal.protocol;

import akka.io.dns.internal.Message$;
import com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.entity.EntryType;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.pb.FlinkProtobufHelper;
import org.apache.flink.pb.PbDeSerUtils;
import org.apache.flink.pb.PbRowSerializationSchema;
import org.apache.flink.pb.PbRowTypeInformation;
import org.apache.flink.pb3.canal.entity.Header;
import org.apache.flink.pb3.canal.entity.Message;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalPbMessageSerliableTest {

  private static Logger logger = LoggerFactory.getLogger(CanalPbMessageSerliableTest.class);


  @Test
  public void testValidateRow(){
    RowType rowType = PbRowTypeInformation.generateRowType(Header.getDescriptor());
    RowData header1 = GenericRowData.of(null,
        0,StringData.fromString("mysql-bin.00011"),
        1024L,21212L,3232L,1024L,
        null,StringData.fromString("notice1"),
        StringData.fromString("tableA"),0L,
        null,null,
        StringData.fromString("fdsfds"));
    header1 = FlinkProtobufHelper.validateRow(header1,rowType);
    System.out.println(header1);
  }

  @Test
  public void testSerializationProtoMessage(){

    int count = 2000;
    RowData header1 = GenericRowData.of(0,StringData.fromString("mysql-bin.00011"),
        1024,21212,3232,1024,
        null,StringData.fromString("notice1"),
        StringData.fromString("tableA"),12,
        null,null,
        StringData.fromString("fdsfds"));

    String text1 = "It's first notice";
    String text2 = "It's second notice";
    byte[] item = text1.getBytes(StandardCharsets.UTF_8);

    byte[] item1 =  text2.getBytes(StandardCharsets.UTF_8);
    //Entry构造RowData类型
    StringData entryData = StringData.fromString(String.valueOf(EntryType.ROWDATA));
    RowData row2 = GenericRowData.of(header1,entryData,item);

    ArrayData entryList = new GenericArrayData(new Object[]{row2});
    ArrayData rawEntries = new GenericArrayData(new Object[]{item});
    RowType rowType = PbRowTypeInformation.generateRowType(Message.getDescriptor());

    RowData rowData = GenericRowData.of(2121212L,entryList,false,rawEntries);
    PbRowSerializationSchema pbRowSerializationSchema = new PbRowSerializationSchema(rowType,"com.alibaba.otter.canal.protocol.entity.Message");
    byte[] result = pbRowSerializationSchema.serialize(rowData);
    logger.info("当前序列化结果为: {}",result);
  }

}
