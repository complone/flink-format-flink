package org.apache.flink;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.flink.pb.PbCodegenMapDes;
import org.apache.flink.pb.PbRowDeserializationSchema;
import org.apache.flink.pb.PbRowTypeInformation;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class PbCodegenMapDesTest {

  @Test
  public void testAssembleMapDesTest(){

    RowType rowType = PbRowTypeInformation.generateRowType(MapTest.getDescriptor());

    MapTest mapTest = MapTest.newBuilder()
        .setTestString("This is test basic type...")
        .putMap("message key","message value")
        .build();
    String[] needImportClassName = {"com.alibaba.otter.canal.protocol.entity.Message"};

    PbRowDeserializationSchema pbRowDeserializationSchema = new PbRowDeserializationSchema(rowType,
        "org.apache.flink.MapTest",needImportClassName,
        true,true);
    try {
      pbRowDeserializationSchema.deserialize(mapTest.toByteArray());
    }catch (Exception exception){
      exception.printStackTrace();
    }
//
//
//    Descriptors.FieldDescriptor fd = ;
//
//    PbCodegenMapDes pbCodegenMapDes = new PbCodegenMapDes()
  }

}
