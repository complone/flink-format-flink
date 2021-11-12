package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import org.apache.flink.pb.PbDeSerUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;

public class ProtoToRowConverter {

  private ScriptEvaluator se; //Java脚本中倒入类以及调用
  private Method parseFromMethod; //用于字节流转换成RowData数据类型
  private Class messageClassName;

  public ProtoToRowConverter(String messageClassName, RowType rowType,boolean ignoreDefaultValues,String[] needSerializationClassNames){
      try {
        Descriptors.Descriptor descriptor = PbDeSerUtils.getDescriptor(messageClassName);
        Class messageClass = Class.forName(messageClassName);

        if (descriptor.getFile().getSyntax() == Syntax.PROTO3){
          ignoreDefaultValues = false;
        }
        se = new ScriptEvaluator();
        se.setParameters(new String[]{"message"},new Class[]{messageClass});
        se.setReturnType(RowData.class);
        se.setDefaultImports(needSerializationClassNames);
        se.setDefaultImports(
            "org.apache.flink.table.data.GenericRowData",
            "org.apache.flink.table.data.GenericArrayData",
            "org.apache.flink.table.data.GenericMapData",
            "org.apache.flink.table.data.RowData",
            "org.apache.flink.table.data.ArrayData",
            "org.apache.flink.table.data.StringData",
            "com.google.protobuf.ByteString",
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

        StringBuilder sb  = new StringBuilder();
        sb.append("RowData rowData=null;");
        PbCodegenDes codegenRowDes = PbCodegenDesFactory.getPbCodegenTopRowDes(descriptor,rowType,ignoreDefaultValues);
        String genCode = codegenRowDes.codegen("rowData","message");
        sb.append(genCode);
        sb.append("return rowData; ");
        String code = sb.toString();
        System.out.println(sb);
        this.messageClassName = messageClass;
        parseFromMethod = messageClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
        se.cook(code);
        System.out.println(parseFromMethod.toGenericString());

      }catch (Exception ex){

      }
  }

  /**
   * 将字节流转换成schema
   * @param data
   * @return
   */
  public RowData convertProtoBinaryToRow(byte[] data)
      throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, CompileException {
    Object messageObj = parseFromMethod.invoke(null,data);
    RowData result = (RowData)se.evaluate(new Object[]{messageObj});
//    RowData ret = (RowData) se.evaluate(new Object[]{messageObj});
//    System.out.format("当前还原出的RowData格式为 %s",ret);
    return result;
  }



}
