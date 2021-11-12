package org.apache.flink.pb;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowToProtoByteArray {
  private static Logger logger = LoggerFactory.getLogger(RowToProtoByteArray.class);


  protected RowType rowType;
  protected List<ProtoSchemaMeta> protoSchemaMetaList = new ArrayList<>();
  protected Descriptors.Descriptor descriptor;
  protected Map<Integer,RowToProtoByteArray> protoIndexToConverterMap = new HashMap<>();

  public RowToProtoByteArray(RowType rowType,Descriptors.Descriptor descriptor){
    this.rowType = rowType;
    this.descriptor = descriptor;

    for(int schemaFieldIndex = 0;schemaFieldIndex< rowType.getFields().size();schemaFieldIndex++){
      //定义一个Proto文件
      RowType.RowField rowField = rowType.getFields().get(schemaFieldIndex);

      Descriptors.FieldDescriptor field = descriptor.findFieldByName(rowField.getName());
      checkNotNull(field);
      LogicalType fieldType = rowField.getType();
      protoSchemaMetaList.add(new ProtoSchemaMeta(schemaFieldIndex,field,fieldType));


      //如果是一个message文件
      if (field.getJavaType() == JavaType.MESSAGE){
        if (field.isMapField()){
          MapType mapType = (MapType) fieldType;
          Descriptors.FieldDescriptor valueFd = field.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

          if (valueFd.getJavaType() == JavaType.MESSAGE) {
            RowToProtoByteArray rowToProtoByteArray = new RowToProtoByteArray(
                (RowType) mapType.getValueType(), valueFd.getMessageType());
            protoIndexToConverterMap.put(field.getNumber(), rowToProtoByteArray);
          }
        }
        //如果是一个数组
        else if(field.isRepeated()){
          ArrayType arrayType = (ArrayType) fieldType;
          RowToProtoByteArray rowToProtoByteArray = new RowToProtoByteArray((RowType) arrayType.getElementType(),field.getMessageType());
          protoIndexToConverterMap.put(field.getNumber(),rowToProtoByteArray);
        }else{
          RowToProtoByteArray rowToProtoByteArray = new RowToProtoByteArray((RowType) fieldType,field.getMessageType());
          protoIndexToConverterMap.put(field.getNumber(),rowToProtoByteArray);
        }
      }
    }

  }

  /**
   * 序列化
   * @param row
   * @return
   */
  public byte[] convertToByteArray(RowData row){
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream stream = CodedOutputStream.newInstance(baos);


    try {
      for (ProtoSchemaMeta protoSchemaMeta : protoSchemaMetaList) {
        Descriptors.FieldDescriptor fd = protoSchemaMeta.getFd();
        int schemaIndex = protoSchemaMeta.getSchemaIndex();
        LogicalType logicalType = protoSchemaMeta.getLogicalType();
        if (!row.isNullAt(schemaIndex)) {
          //消息是一个Message类型，而且包含一个数组
          if (!fd.isRepeated() && !fd.isMapField()) {
            stream.writeTag(fd.getNumber(), fd.getLiteType().getWireType());
            if (fd.getJavaType()  == JavaType.MESSAGE){
              RowData.FieldGetter fieldGetter =RowData.createFieldGetter(logicalType,schemaIndex);
              RowData subRowData = (RowData)fieldGetter.getFieldOrNull(row);
              RowToProtoByteArray subRowToProtoByteArray = protoIndexToConverterMap.get(fd.getNumber());
              writeMessage(stream,subRowToProtoByteArray,subRowData);

            }else{
              writeSimpleObj(stream,row,schemaIndex,fd);
            }
          }else if (fd.isMapField()){
            //map type
            //FIXME 因为protobuf3的消息格式发生了改变，一个proto文件当中会存在多个message文件
            // 其中每个message的字段数据结构 都可以看作是一个数组
            Descriptors.FieldDescriptor keyFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
            Descriptors.FieldDescriptor valueFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);
            MapType mapType = (MapType) protoSchemaMeta.getLogicalType();
            MapData map = row.getMap(schemaIndex);

            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();

            for(int i = 0; i <keys.size(); i++){
              stream.writeTag(fd.getNumber(), WireFormat.WIRETYPE_LENGTH_DELIMITED);
              ByteArrayOutputStream entryPos = new ByteArrayOutputStream();
              CodedOutputStream entryStream = CodedOutputStream.newInstance(entryPos);

              if (!keys.isNullAt(i)){
                entryStream.writeTag(PbConstant.PB_MAP_KEY_TAG,keyFd.getLiteType().getWireType());
                writeSimpleObj(entryStream,keys,i,keyFd);
              }
              if (!values.isNullAt(i)){
                entryStream.writeTag(PbConstant.PB_MAP_VALUE_TAG,valueFd.getLiteType().getWireType());
                if (valueFd.getJavaType() == JavaType.MESSAGE){
                  ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(mapType.getValueType());
                  writeMessage(entryStream,protoIndexToConverterMap.get(elementGetter),rowType);
                }else{
                  writeSimpleObj(entryStream,values,i,valueFd);
                }
              }
              //刷新数据流缓冲区，序列化成功一个元素，写满一个channel的上限值
              entryStream.flush();
              byte[] entryData = baos.toByteArray();
              stream.writeUInt32NoTag(entryData.length);
              stream.writeRawBytes(entryData);
            }

          }
          //如果该proto文件存在list类型
          else if (fd.isRepeated()){
           ArrayData arrayData = row.getArray(schemaIndex);
           for (int j=0;j<arrayData.size(); j++){
             stream.writeTag(fd.getNumber(),fd.getLiteType().getWireType());
             if (fd.getJavaType() == JavaType.MESSAGE) {
               //repeated row
               ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(((ArrayType) logicalType).getElementType());
               elementGetter.getElementOrNull(arrayData, j);
               Object messageElement = elementGetter.getElementOrNull(arrayData, j);
               if(null == messageElement){
                 writeNullMessage(stream, fd.getMessageType());
               }else{
                 writeMessage(stream, protoIndexToConverterMap.get(fd.getNumber()), messageElement);
               }
             } else {
               //repeated simple type
               writeSimpleObj(stream, arrayData, j, fd);
             }
           }
          }
        }
      }
      stream.flush();
      byte[] bytes = baos.toByteArray();
      return bytes;
    }catch (Exception ex){
        ex.printStackTrace();
    }
    return baos.toByteArray();

  }

  public void writeMessage(CodedOutputStream stream,RowToProtoByteArray rowToProtoByteArray,Object obj)
      throws IOException {
    byte[] subBytes = rowToProtoByteArray.convertToByteArray((RowData) obj);
    stream.writeFixed32NoTag(subBytes.length);
    stream.writeRawBytes(subBytes);
  }

  private void writeNullMessage(CodedOutputStream stream, Descriptors.Descriptor messageType) throws IOException {
    byte[] subBytes = messageType.toProto().getDefaultInstanceForType().toByteArray();
    stream.writeFixed32NoTag(subBytes.length);
    stream.writeRawBytes(subBytes);
  }

  /**
   * 根据RowData数据类型的字段的不同类型进行适配
   * @param stream
   * @param row
   * @param position
   * @param fd
   */
  public void writeSimpleObj(CodedOutputStream stream,RowData row,int position,Descriptors.FieldDescriptor fd)
      throws IOException {
    switch (fd.getJavaType()){
      case STRING:
        stream.writeStringNoTag(row.getString(position).toString());
      case INT:
        stream.writeFixed32NoTag(row.getInt(position));
        break;
      case LONG:
        stream.writeFixed64NoTag(row.getLong(position));
        break;
      case FLOAT:
        stream.writeFloatNoTag(row.getFloat(position));
        break;
      case DOUBLE:
        stream.writeDoubleNoTag(row.getDouble(position));
        break;
      case BOOLEAN:
        stream.writeBoolNoTag(row.getBoolean(position));
        break;
      case BYTE_STRING:
        stream.write(row.getByte(position));
        break;
      case ENUM:
        stream.writeEnumNoTag(fd.getEnumType().findValueByName(row.getString(position).toString()).getNumber());
        break;
    }
  }


  /**
   * 对数组类型的各种基本类型进行适配
   * @param stream
   * @param array
   * @param position
   * @param fd
   * @throws IOException
   */
  public void writeSimpleObj(CodedOutputStream stream,ArrayData array,int position,Descriptors.FieldDescriptor fd)
      throws IOException {
    switch (fd.getJavaType()){
      case STRING:
        stream.writeStringNoTag(array.getString(position).toString());
        break;
      case INT:
        stream.writeInt32NoTag(array.getInt(position));
        break;
      case LONG:
        stream.writeInt64NoTag(array.getLong(position));
        break;
      case FLOAT:
        stream.writeDoubleNoTag(array.getFloat(position));
        break;
      case DOUBLE:
        stream.writeDoubleNoTag(array.getDouble(position));
        break;
      case BOOLEAN:
        stream.writeBoolNoTag(array.getBoolean(position));
        break;
      case BYTE_STRING:
        byte[] result = array.getBinary(position);
        ByteString byteValue = ByteString.copyFrom(result);
        stream.writeRawBytes(byteValue);
        break;
      case ENUM:
        Descriptors.EnumValueDescriptor enumValueDescriptor = fd.getEnumType().findValueByName(array.getString(position).toString());
        if (null == enumValueDescriptor){
          int firstEnumTagNum = fd.getEnumType().getValues().get(0).getNumber();
          stream.writeEnumNoTag(firstEnumTagNum);
        }else{
          stream.writeEnumNoTag(enumValueDescriptor.getNumber());
        }
    }

  }

  /**
   * 设置protobuf的表字段
   */
  public class ProtoSchemaMeta{
    private int schemaIndex; //设置数据包中的字段位置
    private Descriptors.FieldDescriptor fd; //读取字段的文件描述符
    private LogicalType logicalType;//映射成表的逻辑执行计划类型

    public ProtoSchemaMeta(int schemaIndex,Descriptors.FieldDescriptor fd,LogicalType logicalType){
      this.schemaIndex = schemaIndex;
      this.fd = fd;
      this.logicalType = logicalType;
    }

    public int getSchemaIndex() {
      return schemaIndex;
    }

    public void setSchemaIndex(int schemaIndex) {
      this.schemaIndex = schemaIndex;
    }

    public FieldDescriptor getFd() {
      return fd;
    }

    public void setFd(FieldDescriptor fd) {
      this.fd = fd;
    }

    public LogicalType getLogicalType() {
      return logicalType;
    }

    public void setLogicalType(LogicalType logicalType) {
      this.logicalType = logicalType;
    }
  }

}
