// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: message.proto

package org.apache.flink.pb3.canal.entity;

public interface ColumnOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.alibaba.otter.canal.protocol.Column)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   **字段下标 UPDATE DELETE *
   * </pre>
   *
   * <code>int32 index = 1;</code>
   * @return The index.
   */
  int getIndex();

  /**
   * <pre>
   **字段java类型 *
   * </pre>
   *
   * <code>int32 sqlType = 2;</code>
   * @return The sqlType.
   */
  int getSqlType();

  /**
   * <pre>
   **字段名称 *
   * </pre>
   *
   * <code>string name = 3;</code>
   * @return The name.
   */
  java.lang.String getName();
  /**
   * <pre>
   **字段名称 *
   * </pre>
   *
   * <code>string name = 3;</code>
   * @return The bytes for name.
   */
  com.google.protobuf.ByteString
      getNameBytes();
}
