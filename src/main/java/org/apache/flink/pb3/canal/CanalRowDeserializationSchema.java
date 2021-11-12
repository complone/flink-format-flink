package org.apache.flink.pb3.canal;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.pb.PbRowDeserializationSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import scala.math.Ordering;
public class CanalRowDeserializationSchema implements DeserializationSchema<RowData> {

  private static final long serialVersionUID =  -21213443439L;
  private static final String OP_INSERT = "INSERT";
  private static final String OP_UPDATE = "UPDATE";
  private static final String OP_DELETE = "DELETE";

  private final PbRowDeserializationSchema pbRowDeserializationSchema;
  private final String messageClassName;
  private final boolean ignoreParseErrors;
  private final int fieldCount;

  //TypeInformations的消息格式化成元组或者其他类型
  public CanalRowDeserializationSchema(RowType rowType,String messageClassName,boolean ignoreParseErrors,
      TimestampFormat timeStampFormat,String[] needSerializationClass) {
    this.messageClassName = messageClassName;
    this.fieldCount = rowType.getFieldCount();
    this.ignoreParseErrors = ignoreParseErrors;
    this.pbRowDeserializationSchema = new PbRowDeserializationSchema(rowType,messageClassName,needSerializationClass,ignoreParseErrors,false);

  }


  @Override
  public RowData deserialize(byte[] message) throws IOException {
    RowData row = this.pbRowDeserializationSchema.deserialize(message);
    return row;
  }

  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
//    String type = row.getString(2).toString();
//    ArrayData data;
//    RowData insert;
//    if ("INSERT".equals(type)){
//      data = row.getArray(0);
//
//      for(int i=0;i<data.size();i++){
//        insert = data.getRow(i,this.fieldCount);
//        insert.setRowKind(RowKind.DELETE);
//      }
//    }
//    //changelog是delete的话 ，不能允许下一条change是更新操作
//    else if(!"UPDATE".equals(type)){
//      if ("DELETE".equals(type)){
//        data = row.getArray(0);
//        for (int i=0;i<data.size();++i){
//          insert = data.getRow(i,this.fieldCount);
//          insert.setRowKind(RowKind.DELETE);
//        }
//      }else if(!ignoreParseErrors){
//        throw new IOException(java.lang.String.format("Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",type,new java.lang.String(message)));
//      }else {
//        //修改前，删除前的canal增量数据
//        data = row.getArray(0);
//        //修改后，新增后的canal增量数据
//        ArrayData old = row.getArray(1);
//        for (int i =0;i<data.size();i++){
//          GenericRowData after = (GenericRowData) data.getRow(i,this.fieldCount);
//          GenericRowData before = (GenericRowData) old.getRow(i,this.fieldCount);
//
//          //修改前，删除前的增量数据进行解析
//          for (int f=0;f<this.fieldCount;f++){
//            if (before.isNullAt(f)){
//              before.setField(f,after.getField(f));
//            }
//          }
//
//          before.setRowKind(RowKind.UPDATE_BEFORE);
//          after.setRowKind(RowKind.UPDATE_AFTER);
//        }
//      }
//    }
    DeserializationSchema.super.deserialize(message,out);
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
