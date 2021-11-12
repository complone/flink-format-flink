// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: message.proto

package com.alibaba.otter.canal.protocol.entity;

/**
 * <pre>
 **打散后的事件类型，主要用于标识事务的开始，变更数据，结束*
 * </pre>
 *
 * Protobuf enum {@code com.alibaba.otter.canal.protocol.EntryType}
 */
public enum EntryType
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>ENTRYTYPECOMPATIBLEPROTO2 = 0;</code>
   */
  ENTRYTYPECOMPATIBLEPROTO2(0),
  /**
   * <code>TRANSACTIONBEGIN = 1;</code>
   */
  TRANSACTIONBEGIN(1),
  /**
   * <code>ROWDATA = 2;</code>
   */
  ROWDATA(2),
  /**
   * <code>TRANSACTIONEND = 3;</code>
   */
  TRANSACTIONEND(3),
  /**
   * <pre>
   ** 心跳类型，内部使用，外部暂不可见，可忽略 *
   * </pre>
   *
   * <code>HEARTBEAT = 4;</code>
   */
  HEARTBEAT(4),
  /**
   * <code>GTIDLOG = 5;</code>
   */
  GTIDLOG(5),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>ENTRYTYPECOMPATIBLEPROTO2 = 0;</code>
   */
  public static final int ENTRYTYPECOMPATIBLEPROTO2_VALUE = 0;
  /**
   * <code>TRANSACTIONBEGIN = 1;</code>
   */
  public static final int TRANSACTIONBEGIN_VALUE = 1;
  /**
   * <code>ROWDATA = 2;</code>
   */
  public static final int ROWDATA_VALUE = 2;
  /**
   * <code>TRANSACTIONEND = 3;</code>
   */
  public static final int TRANSACTIONEND_VALUE = 3;
  /**
   * <pre>
   ** 心跳类型，内部使用，外部暂不可见，可忽略 *
   * </pre>
   *
   * <code>HEARTBEAT = 4;</code>
   */
  public static final int HEARTBEAT_VALUE = 4;
  /**
   * <code>GTIDLOG = 5;</code>
   */
  public static final int GTIDLOG_VALUE = 5;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static EntryType valueOf(int value) {
    return forNumber(value);
  }

  /**
   * @param value The numeric wire value of the corresponding enum entry.
   * @return The enum associated with the given numeric wire value.
   */
  public static EntryType forNumber(int value) {
    switch (value) {
      case 0: return ENTRYTYPECOMPATIBLEPROTO2;
      case 1: return TRANSACTIONBEGIN;
      case 2: return ROWDATA;
      case 3: return TRANSACTIONEND;
      case 4: return HEARTBEAT;
      case 5: return GTIDLOG;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<EntryType>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      EntryType> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<EntryType>() {
          public EntryType findValueByNumber(int number) {
            return EntryType.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalStateException(
          "Can't get the descriptor of an unrecognized enum value.");
    }
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return com.alibaba.otter.canal.protocol.entity.MessageOuterClass.getDescriptor().getEnumTypes().get(1);
  }

  private static final EntryType[] VALUES = values();

  public static EntryType valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private EntryType(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:com.alibaba.otter.canal.protocol.EntryType)
}

