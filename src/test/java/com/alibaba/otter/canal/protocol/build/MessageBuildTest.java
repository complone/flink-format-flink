package com.alibaba.otter.canal.protocol.build;


import com.alibaba.otter.canal.protocol.entity.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.entity.EntryType;
import com.alibaba.otter.canal.protocol.entity.Header;
import com.alibaba.otter.canal.protocol.entity.Message;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MessageBuildTest {

  @Test
  public void testMessageBuild(){

    int batchSize = 2000000;
    int count = 20000;

    Header.Builder headerBuilder = Header.newBuilder();
    headerBuilder.setLogFileName("mysql-bin.00000"+String.valueOf(count));
    headerBuilder.setLogFileOffset(1024);
    headerBuilder.setExecuteTime(1024);
    //组装消费位点
    String notice = "I'am recevied message,Current Message ID:"+count;

    String notice1 = "this is second notice";
    ByteString item = ByteString.copyFrom(notice.getBytes(StandardCharsets.UTF_8));
    ByteString item1 = ByteString.copyFrom(notice1.getBytes(StandardCharsets.UTF_8));

    Entry.Builder entryBuilder = Entry.newBuilder();
    entryBuilder.setHeader(headerBuilder.build());
    entryBuilder.setEntryType(EntryType.ROWDATA);
    entryBuilder.setStoreValue(item);
    Entry entry = entryBuilder.build();

    Entry.Builder entryBuilder1 = Entry.newBuilder();
    entryBuilder1.setHeader(headerBuilder.build());
    entryBuilder1.setEntryType(EntryType.ROWDATA);
    entryBuilder1.setStoreValue(item1);
    Entry entry1 = entryBuilder1.build();


    List<Entry> list = new ArrayList<>();
    list.add(entry);
    list.add(entry1);
    List<ByteString> rawEntries = new ArrayList<>();
    rawEntries.add(item);
    rawEntries.add(item1);
    Message message = Message.newBuilder()
        .addAllEntries(list)
        .addAllRawEntries(rawEntries)
        .setRaw(true)
        .setId(10000L)
        .setRawEntries(1,item).build();
    System.out.println(message.toString());
  }

}
