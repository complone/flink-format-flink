package com.alibaba.otter.canal.protocol.canal;

import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.pb3.canal.entity.CanalPacket.PacketType;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka Message类的序列化
 *
 * @author rewerma 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class KafkaMessageSerializer implements Serializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        byte[] transpots = new byte[0];
        try {
            transpots = this.buildData(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return transpots;
    }

    @Override
    public void close() {
        // nothing to do
    }

    /**
     * Canal protobuf的报文格式
     *  messageSize | rowEntry,rowEntry,rowEntry | rowEnrty.size()
     *  ACK状态位 | 4/5位(是否超过整型) 消息 | messageSize(不超过整形范围按照4/5字节位存)
     *
     * 组装后的报文格式
     * 5个字节位存类型范围长度(string,bytes,embadded messages) |
     * messageSize | 消息序号(1个字节位) | rowEntry,rowEntry,rowEntry(每条entry2个字节位)
     */
    public byte[] buildData(Message message) throws IOException {
        List<ByteString> rowEntries = message.getRawEntries();
        //保存即将写入的每条消息

        //统计消息大小
        int messageSize = 0;
        messageSize = CodedOutputStream.computeInt64Size(1,message.getId());

        int dataSize = 0;
        for(ByteString rowEntry: rowEntries){
            dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
        }

        messageSize +=dataSize;
        messageSize += 1*rowEntries.size();


        //封装数据包
        int size = 0;
        size += CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber());
        //跳过5个字节
        size += CodedOutputStream.computeTagSize(5)
            + CodedOutputStream.computeUInt32SizeNoTag(messageSize) + messageSize;

        byte[] body  = new byte[size];
        CodedOutputStream out  = CodedOutputStream.newInstance(body);
        out.writeEnum(3, PacketType.MESSAGES.getNumber());
        out.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        out.writeInt32NoTag(messageSize);

        //写入消息
        out.writeInt64(1,message.getId());
        for(ByteString rowEntry: rowEntries){
            out.writeBytes(2,rowEntry);
        }
        out.checkNoSpaceLeft();
        return body;

    }

}
