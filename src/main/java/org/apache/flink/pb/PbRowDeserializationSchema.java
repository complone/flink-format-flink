package org.apache.flink.pb;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.pb3.canal.CanalRowDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PbRowDeserializationSchema implements DeserializationSchema<RowData> {


  private static Logger LOG = LoggerFactory.getLogger(PbRowDeserializationSchema.class);
  private static final long serialVersionUID = -4040917522067315718L;

  private  final RowType rowType;//需要通过Flink planner进行解析

  private final String messageClassName; //解析需要反序列化的消息

  private final boolean ignoreParseErrors; //是否是解析时候非法的错误
  private final boolean ignoreDefaultValues; // 是否是属于默认值

  private final String[] needSerializationClassNames; //需要填充的序列化类

  private ProtoToRowConverter protoToRowConverter;


  public PbRowDeserializationSchema(RowType rowType,String messageClassName,String[] importClassName,
      boolean ignoreParseErrors,boolean ignoreDefaultValues) {
    this.rowType = rowType;
    this.messageClassName = messageClassName;
    this.ignoreParseErrors = ignoreParseErrors;
    this.ignoreDefaultValues = ignoreDefaultValues;
    this.needSerializationClassNames = importClassName;
    Descriptor descriptor = PbDeSerUtils.getDescriptor(messageClassName);
    PbSchemaValidator pbSchemaValidator = new PbSchemaValidator(descriptor,rowType);
    pbSchemaValidator.validate();
    this.protoToRowConverter = new ProtoToRowConverter(messageClassName,rowType,ignoreParseErrors,importClassName);


  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.protoToRowConverter = new ProtoToRowConverter(messageClassName,rowType,ignoreDefaultValues,needSerializationClassNames);
  }

  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
    DeserializationSchema.super.deserialize(message, out);
  }

  @Override
  public RowData deserialize(byte[] bytes) throws IOException {
    try {

      RowData rowData = this.protoToRowConverter.convertProtoBinaryToRow(bytes);
      return rowData;
    } catch (Throwable t) {
      if (ignoreParseErrors){
        return null;
      }
      LOG.error("Failed to deserialize PB object.", t);
      throw new IOException("Failed to deserialize PB object.", t);
    }
  }

  @Override
  public boolean isEndOfStream(RowData rowData) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return null;
  }
}
