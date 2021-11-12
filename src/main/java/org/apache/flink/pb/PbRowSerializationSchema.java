package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PbRowSerializationSchema implements SerializationSchema<RowData> {


  private static final Logger logger = LoggerFactory.getLogger(PbRowSerializationSchema.class);

  private final RowType rowType;

  private final String messageClassName;

  private transient RowToProtoByteArray rowToProtoByteArray;

  public PbRowSerializationSchema(RowType rowType,String messageClassName){
    this.rowType = rowType;
    this.messageClassName = messageClassName;
    Descriptors.Descriptor descriptor = PbDeSerUtils.getDescriptor(messageClassName);
    PbSchemaValidator pbSchemaValidator = new PbSchemaValidator(descriptor,rowType);
    pbSchemaValidator.validate();
    this.rowToProtoByteArray = new RowToProtoByteArray(rowType,descriptor);
  }

  @Override
  public byte[] serialize(RowData rowData) {
    byte[] result = rowToProtoByteArray.convertToByteArray(rowData);
    return result;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.rowToProtoByteArray = new RowToProtoByteArray(rowType,PbDeSerUtils.getDescriptor(messageClassName));
  }
}
