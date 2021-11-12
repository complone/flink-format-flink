package org.apache.flink.pb;

import java.util.concurrent.atomic.AtomicInteger;

public class CodegenVarUid {
  private static CodegenVarUid codegenVarUid = new CodegenVarUid();
  private AtomicInteger atomicInteger = new AtomicInteger();

  public CodegenVarUid(){

  }

  public static CodegenVarUid getInstance(){
    return codegenVarUid;
  }

  public int getAndIncr(){
    return atomicInteger.getAndIncrement();
  }

}
