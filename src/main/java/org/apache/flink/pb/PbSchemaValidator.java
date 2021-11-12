package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class PbSchemaValidator {

    private Descriptors.Descriptor descriptor;
    private RowType rowType;
    private Map<JavaType, List<LogicalTypeRoot>> matchTypes; //Java类型映射成表的逻辑计划类型

  public PbSchemaValidator(Descriptors.Descriptor descriptor, RowType rowType) {
    this.descriptor = descriptor;
    this.rowType = rowType;
    this.matchTypes = new HashMap<>();
    matchTypes.put(JavaType.BOOLEAN, Collections.singletonList(LogicalTypeRoot.BOOLEAN));
    matchTypes.put(JavaType.BYTE_STRING, Arrays.asList(LogicalTypeRoot.BINARY, LogicalTypeRoot.VARBINARY));
    matchTypes.put(JavaType.DOUBLE, Collections.singletonList(LogicalTypeRoot.DOUBLE));
    matchTypes.put(JavaType.FLOAT, Collections.singletonList(LogicalTypeRoot.FLOAT));
    matchTypes.put(JavaType.ENUM, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
    matchTypes.put(JavaType.STRING, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
    matchTypes.put(JavaType.INT, Collections.singletonList(LogicalTypeRoot.INTEGER));
    matchTypes.put(JavaType.LONG, Collections.singletonList(LogicalTypeRoot.BIGINT));
  }


  public Descriptors.Descriptor getDescriptor(){
    return descriptor;
  }

  public void validateTypeMatch(Descriptors.Descriptor descriptor,RowType rowType){
    rowType.getFields().forEach( field -> {
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getName());
      validateTypeMatch(fieldDescriptor,field.getType());
    });
  }

  public void validateTypeMatch(FieldDescriptor fd,LogicalType logicalType){
    if (!fd.isRepeated()){
      //如果该字段是protobuf当中的拓展字段
      if (fd.getJavaType() != JavaType.MESSAGE){
          validateSimpleType(fd,logicalType.getTypeRoot());
      } else {
        validateTypeMatch(fd.getMessageType(),(RowType)logicalType);
      }
    }
  }

  private void validateSimpleType(FieldDescriptor fd,LogicalTypeRoot logicalTypeRoot){
    if (!matchTypes.containsKey(fd.getJavaType())){
      throw new ValidationException("unsupported protobuf java type: " + fd.getJavaType());
    }

    if (!matchTypes.get(fd.getJavaType()).stream().filter(x -> x == logicalTypeRoot).findAny().isPresent()){
      throw new ValidationException("protobuf field type does not match column, "+fd.getJavaType() + "(pb) is not compatible of "+logicalTypeRoot + "(table)");
    }

  }

  public void validate(){
    validateTypeMatch(descriptor,rowType);
  }



}
