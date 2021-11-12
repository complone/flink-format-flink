package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.List;
import jdk.nashorn.internal.runtime.logging.DebugLogger;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 生成一个RowType类型
 */

public class PbRowTypeInformation {

  private static Logger logger = LoggerFactory.getLogger(PbRowTypeInformation.class);

  public static MapType generateMapType(Descriptors.FileDescriptor root){
    List<Descriptor> fieldList =  root.getMessageTypes();
    int size = fieldList.size();
    for (int i =0;i<size;i++){
      Descriptor descriptor = fieldList.get(i);
      List<FieldDescriptor> fieldDescriptors =  descriptor.getFields();
      System.out.println("当前遍历到的枚举类型为: "+descriptor.getName());
    }

    logger.info("当前proto文件解析结果: {} ", fieldList);
    return null;
  }

  /**
   * 生成RowData类型
   * @param root 传入的Desciptor  Message单位的描述符
   * @return
   */
  public static RowType generateRowType(Descriptors.Descriptor root){
    int size = root.getFields().size();
    //待解析的字段类型
    LogicalType[] types = new LogicalType[size];
    String[] rowFieldNames = new String[size];

    for(int i=0;i<size;i++){
      FieldDescriptor field = root.getFields().get(i);
      rowFieldNames[i] = field.getName();
      types[i] = generateLogical(field);

    }
    return RowType.of(types,rowFieldNames);

  }

  /**
   * 解析该字段需要桥接的类型
   * @return
   */
  public static LogicalType generateLogical(Descriptors.FieldDescriptor field){
    JavaType fieldType = field.getJavaType();
    LogicalType type;
    if (fieldType.equals(JavaType.MESSAGE)){
      if (field.isMapField()){
        MapType mapType = new MapType(
            generateLogical(field.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME)),
                generateLogical(field.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME)));
        return mapType;
      }else if(field.isRepeated()){
        return new ArrayType(generateRowType(field.getMessageType()));
      }else{
        return generateRowType(field.getMessageType());
      }
    }
    if (fieldType.equals(JavaType.STRING)){
      type =  new VarCharType(Integer.MAX_VALUE);
    }else if (fieldType.equals(JavaType.LONG)){
      type =  new BigIntType();
    }else if (fieldType.equals(JavaType.BOOLEAN)){
      type =  new BooleanType();
    }else if(fieldType.equals(JavaType.INT)){
      type =  new VarCharType(Integer.MAX_VALUE);
    }else if (fieldType.equals(JavaType.DOUBLE)){
      type =  new DoubleType();
    }else if (fieldType.equals(JavaType.FLOAT)){
      type =  new FloatType();
    }else if(fieldType.equals(JavaType.ENUM)){
      type = new VarCharType(Integer.MAX_VALUE);
    }else if (fieldType.equals(JavaType.BYTE_STRING)){
      type = new VarBinaryType(Integer.MAX_VALUE);
    }else{
      throw new ValidationException("unsupported field type:"+fieldType);
    }

    if (field.isRepeated()){
      return new ArrayType(type);
    }
    return type;
  }
}
