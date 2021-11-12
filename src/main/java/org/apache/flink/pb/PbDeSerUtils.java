package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import java.lang.reflect.Method;
import org.apache.flink.table.types.logical.LogicalType;

public class PbDeSerUtils {

  public static final String OUTER_CLASS = "OuterClass";

  /**
   * 解析protobuf文件字段
   * @param name
   * @return
   */
  public static String fieldNameToJsonName(String name){
    final int length = name.length();
    StringBuilder result = new StringBuilder(length);
    boolean isNextUpperCase = false;//驼峰命名检查
    for(int i=0;i<length;i++){
      char ch = name.charAt(i);
      if (ch == '_'){
        isNextUpperCase = true;
      }else if(isNextUpperCase){
        if ('a'<=ch && ch<='z'){
          isNextUpperCase = false;
        }
        result.append(ch);
      }else {
        result.append(ch);
      }
    }
    return result.toString();
  }

  public static boolean isSimpleType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        return true;
      default:
        return false;
    }
  }

  /**
   * 通过protobuf的文件描述符反射具体的字节码操作方法
   * @param className
   * @return
   */
  public static Descriptors.Descriptor getDescriptor(String className){
    try{
      Class<?> pbClass = Class.forName(className);
      Object object = pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
      return (Descriptors.Descriptor)pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
    }catch (Exception ex){
      throw new IllegalArgumentException(String.format("get %s descriptors error!"),ex);
    }
  }

  public static String getStrongCameCaseJsonName(String name){
    final int length = name.length();
    StringBuilder sb = new StringBuilder(length);
    boolean isNextUpperCase = false;
    name = name.substring(0,1).toUpperCase()+name.substring(1,name.length());
    for (int i=0;i<length;i++){
      char ch  = name.charAt(i);
      if (ch == '_'){
        isNextUpperCase = true;
      }else if (isNextUpperCase){
        if ('a' <=ch && ch<= 'z'){
          ch = (char) (ch - 'a'+'A');
          isNextUpperCase = false;
        }
        sb.append(ch);
      }else {
        sb.append(ch);
      }
    }
    return sb.toString();

  }

  /**
   * 做语法检查
   * @param name 解析字段名是否合法
   * @return
   */
  public static String getStrongUpperCaseName(String name){
    String fieldName = getStrongCameCaseJsonName(name);
    if (fieldName.length() == 1){
      return fieldName.toUpperCase();
    }else{
      return fieldName.substring(0,1).toUpperCase() + fieldName.substring(1);
    }
  }

  public static String getTypeStrFromFD(Descriptors.FieldDescriptor fd){
    switch (fd.getJavaType()){
      case MESSAGE:
//        return getJavax
        return getJavaFullName(fd.getMessageType());
      case INT:
        return "Integer";
      case LONG:
        return "Long";
      case STRING:
      case ENUM:
        return "Object";
      case DOUBLE:
        return "Double";
      case BYTE_STRING:
        return "ByteString";
      default:
        throw new PbDecodeCodegenException("do not support field type: "+fd.getJavaType());
    }
  }

  public static String getJavaFullName(Descriptors.Descriptor descriptor){
    String javaPackageName = descriptor.getFile().getOptions().getJavaPackage();
    //判断protobuf3是否开启了多文件并行编译
    if (descriptor.getFile().getOptions().getJavaMultipleFiles()){
      if (null !=descriptor.getContainingType()){
        //nested type 是否是嵌套类型
        String parentJavaFullName  = getJavaFullName(descriptor.getContainingType());
        return parentJavaFullName + "." + descriptor.getName();
      }else{
        //如果没有出现类嵌套 则类最多只有一层成员
        return javaPackageName + "." + descriptor.getName();
      }
    }else {
      if (null!=descriptor.getContainingType()){
        String parentJavaFullName = getJavaFullName(descriptor.getContainingType());
        return parentJavaFullName + "." + descriptor.getName();
      }else {
        if (!descriptor.getFile().getOptions().hasJavaOuterClassname()){
          return javaPackageName + "." + descriptor.getName() + OUTER_CLASS + "." + descriptor.getName();
        }else{
          String outerName = descriptor.getFile().getOptions().getJavaOuterClassname();
          return javaPackageName + "." + outerName + "." + descriptor.getName();
        }
      }
    }
  }

}
