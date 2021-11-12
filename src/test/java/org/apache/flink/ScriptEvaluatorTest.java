package org.apache.flink;

import com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.entity.EntryType;
import com.alibaba.otter.canal.protocol.entity.Header;
import com.alibaba.otter.canal.protocol.entity.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ScriptEvaluatorTest {

  @Test
  public void testSeEvalInvoke()
      throws InvocationTargetException, CompileException, InvalidProtocolBufferException {

    int count =4000;

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


    String messageClassName = "com.alibaba.otter.canal.protocol.entity.Message";
    Class messageClass = null;
    try {
      messageClass = Class.forName(messageClassName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    ScriptEvaluator se = new ScriptEvaluator();
    se.setParameters(new String[]{"message"},new Class[]{messageClass});
    se.setReturnType(Message.class);
    se.setDefaultImports(
        "org.apache.flink.table.data.GenericRowData",
        "org.apache.flink.table.data.GenericArrayData",
        "org.apache.flink.table.data.GenericMapData",
        "org.apache.flink.table.data.RowData",
        "org.apache.flink.table.data.ArrayData",
        "org.apache.flink.table.data.StringData",
        "java.lang.Integer",
        "java.lang.Long",
        "java.lang.Float",
        "java.lang.Double",
        "java.lang.Boolean",
        "java.util.ArrayList",
        "java.sql.Timestamp",
        "java.util.List",
        "java.util.Map",
        "java.util.HashMap");
    se.cook("RowData rowData=null;com.alibaba.otter.canal.protocol.entity.Message message0 = message;return message0;");
    Message message1 = (Message)se.evaluate(new Object[]{message});
    System.out.println(message1);

//    se.cook(
//       "RowData rowData=null;com.alibaba.otter.canal.protocol.entity.Message message0 = message;GenericRowData rowData0 = new GenericRowData(4);Object returnVar1 = null;returnVar1 = Long.valueOf(message0.getId());rowData0.setField(0, returnVar1);Object returnVar2 = null;List<com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry> list0=message0.getEntriesList();Object[] newObjs0 = new Object[list0.size()];for(int i0 = 0;i0 < list0.size(); i0++){Object returnVar0 = null;com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry subObj0 = (com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry)list0.get(i0);com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry message0 = subObj0;GenericRowData rowData0 = new GenericRowData(3);Object returnVar1 = null;com.alibaba.otter.canal.protocol.entity.Header message0 = message0.getHeader();GenericRowData rowData0 = new GenericRowData(13);Object returnVar1 = null;returnVar1 = Integer.valueOf(message0.getVersion());rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getLogFileName().toString());rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = Long.valueOf(message0.getLogFileOffset());rowData0.setField(2, returnVar3);Object returnVar4 = null;returnVar4 = Long.valueOf(message0.getServerId());rowData0.setField(3, returnVar4);Object returnVar5 = null;returnVar5 = Long.valueOf(message0.getServerenCode());rowData0.setField(4, returnVar5);Object returnVar6 = null;returnVar6 = Long.valueOf(message0.getExecuteTime());rowData0.setField(5, returnVar6);Object returnVar7 = null;returnVar7 = StringData.fromString(message0.getSourceType().toString());rowData0.setField(6, returnVar7);Object returnVar8 = null;returnVar8 = StringData.fromString(message0.getSchemaName().toString());rowData0.setField(7, returnVar8);Object returnVar9 = null;returnVar9 = StringData.fromString(message0.getTableName().toString());rowData0.setField(8, returnVar9);Object returnVar10 = null;returnVar10 = Long.valueOf(message0.getEventLength());rowData0.setField(9, returnVar10);Object returnVar11 = null;returnVar11 = StringData.fromString(message0.getEventType().toString());rowData0.setField(10, returnVar11);Object returnVar12 = null;List<com.alibaba.otter.canal.protocol.entity.Pair> list1=message0.getPropsList();Object[] newObjs1 = new Object[list1.size()];for(int i1 = 0;i1 < list1.size(); i1++){Object returnVar1 = null;com.alibaba.otter.canal.protocol.entity.Pair subObj1 = (com.alibaba.otter.canal.protocol.entity.Pair)list1.get(i1);com.alibaba.otter.canal.protocol.entity.Pair message0 = subObj1;GenericRowData rowData0 = new GenericRowData(2);Object returnVar1 = null;returnVar1 = StringData.fromString(message0.getKey().toString());rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getValue().toString());rowData0.setField(1, returnVar2);returnVar1 = rowData0;newObjs1[i1]=returnVar1;}returnVar12 = new GenericArrayData(newObjs1);rowData0.setField(11, returnVar12);Object returnVar13 = null;returnVar13 = StringData.fromString(message0.getGtid().toString());rowData0.setField(12, returnVar13);returnVar1 = rowData0;rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getEntryType().toString());rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = message0.getStoreValue().toByteArray();rowData0.setField(2, returnVar3);returnVar0 = rowData0;newObjs0[i0]=returnVar0;}returnVar2 = new GenericArrayData(newObjs0);rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = Boolean.valueOf(message0.getRaw());rowData0.setField(2, returnVar3);Object returnVar4 = null;List<byte[]> list2=message0.getRawEntriesList();Object[] newObjs2 = new Object[list2.size()];for(int i2 = 0;i2 < list2.size(); i2++){Object returnVar2 = null;byte[] subObj2 = (byte[])list2.get(i2);returnVar2 = subObj2.toByteArray();newObjs2[i2]=returnVar2;}returnVar4 = new GenericArrayData(newObjs2);rowData0.setField(3, returnVar4);RowData = rowData0;return rowData; ");
//    se.evaluate(new Object[]{message});

//    String result = "RowData rowData=null;com.alibaba.otter.canal.protocol.entity.Message message0 = message;GenericRowData rowData0 = new GenericRowData(4);Object returnVar1 = null;returnVar1 = Long.valueOf(message0.getId());rowData0.setField(0, returnVar1);Object returnVar2 = null;List<com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry> list0=message0.getEntriesList();Object[] newObjs0 = new Object[list0.size()];for(int i0 = 0;i0 < list0.size(); i0++){Object returnVar0 = null;com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry subObj0 = (com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry)list0.get(i0);com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry message0 = subObj0;GenericRowData rowData0 = new GenericRowData(3);Object returnVar1 = null;com.alibaba.otter.canal.protocol.entity.Header message0 = message0.getHeader();GenericRowData rowData0 = new GenericRowData(13);Object returnVar1 = null;returnVar1 = Integer.valueOf(message0.getVersion());rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getLogFileName().toString());rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = Long.valueOf(message0.getLogFileOffset());rowData0.setField(2, returnVar3);Object returnVar4 = null;returnVar4 = Long.valueOf(message0.getServerId());rowData0.setField(3, returnVar4);Object returnVar5 = null;returnVar5 = Long.valueOf(message0.getServerenCode());rowData0.setField(4, returnVar5);Object returnVar6 = null;returnVar6 = Long.valueOf(message0.getExecuteTime());rowData0.setField(5, returnVar6);Object returnVar7 = null;returnVar7 = StringData.fromString(message0.getSourceType().toString());rowData0.setField(6, returnVar7);Object returnVar8 = null;returnVar8 = StringData.fromString(message0.getSchemaName().toString());rowData0.setField(7, returnVar8);Object returnVar9 = null;returnVar9 = StringData.fromString(message0.getTableName().toString());rowData0.setField(8, returnVar9);Object returnVar10 = null;returnVar10 = Long.valueOf(message0.getEventLength());rowData0.setField(9, returnVar10);Object returnVar11 = null;returnVar11 = StringData.fromString(message0.getEventType().toString());rowData0.setField(10, returnVar11);Object returnVar12 = null;List<com.alibaba.otter.canal.protocol.entity.Pair> list1=message0.getPropsList();Object[] newObjs1 = new Object[list1.size()];for(int i1 = 0;i1 < list1.size(); i1++){Object returnVar1 = null;com.alibaba.otter.canal.protocol.entity.Pair subObj1 = (com.alibaba.otter.canal.protocol.entity.Pair)list1.get(i1);com.alibaba.otter.canal.protocol.entity.Pair message0 = subObj1;GenericRowData rowData0 = new GenericRowData(2);Object returnVar1 = null;returnVar1 = StringData.fromString(message0.getKey().toString());rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getValue().toString());rowData0.setField(1, returnVar2);returnVar1 = rowData0;newObjs1[i1]=returnVar1;}returnVar12 = new GenericArrayData(newObjs1);rowData0.setField(11, returnVar12);Object returnVar13 = null;returnVar13 = StringData.fromString(message0.getGtid().toString());rowData0.setField(12, returnVar13);returnVar1 = rowData0;rowData0.setField(0, returnVar1);Object returnVar2 = null;returnVar2 = StringData.fromString(message0.getEntryType().toString());rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = message0.getStoreValue().toByteArray();rowData0.setField(2, returnVar3);returnVar0 = rowData0;newObjs0[i0]=returnVar0;}returnVar2 = new GenericArrayData(newObjs0);rowData0.setField(1, returnVar2);Object returnVar3 = null;returnVar3 = Boolean.valueOf(message0.getRaw());rowData0.setField(2, returnVar3);Object returnVar4 = null;List<byte[]> list2=message0.getRawEntriesList();Object[] newObjs2 = new Object[list2.size()];for(int i2 = 0;i2 < list2.size(); i2++){Object returnVar2 = null;byte[] subObj2 = (byte[])list2.get(i2);returnVar2 = subObj2.toByteArray();newObjs2[i2]=returnVar2;}returnVar4 = new GenericArrayData(newObjs2);rowData0.setField(3, returnVar4);RowData = rowData0;return rowData;";
//    String[] debugLines = result.split(";");
//    StringBuilder sb = new StringBuilder();
//    for (String debugLine:debugLines){
//      sb.append(debugLine+"\n");
//    }
//    System.out.println(sb.toString());
  }

}
