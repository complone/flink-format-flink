// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: message.proto

package org.apache.flink.pb3.canal.entity;

public interface MessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.alibaba.otter.canal.protocol.Message)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int64 id = 1;</code>
   * @return The id.
   */
  long getId();

  /**
   * <code>repeated .com.alibaba.otter.canal.protocol.CanalEntry.Entry entries = 2;</code>
   */
  java.util.List<org.apache.flink.pb3.canal.entity.CanalEntry.Entry> 
      getEntriesList();
  /**
   * <code>repeated .com.alibaba.otter.canal.protocol.CanalEntry.Entry entries = 2;</code>
   */
  org.apache.flink.pb3.canal.entity.CanalEntry.Entry getEntries(int index);
  /**
   * <code>repeated .com.alibaba.otter.canal.protocol.CanalEntry.Entry entries = 2;</code>
   */
  int getEntriesCount();
  /**
   * <code>repeated .com.alibaba.otter.canal.protocol.CanalEntry.Entry entries = 2;</code>
   */
  java.util.List<? extends org.apache.flink.pb3.canal.entity.CanalEntry.EntryOrBuilder> 
      getEntriesOrBuilderList();
  /**
   * <code>repeated .com.alibaba.otter.canal.protocol.CanalEntry.Entry entries = 2;</code>
   */
  org.apache.flink.pb3.canal.entity.CanalEntry.EntryOrBuilder getEntriesOrBuilder(
      int index);

  /**
   * <code>bool raw = 3;</code>
   * @return The raw.
   */
  boolean getRaw();

  /**
   * <code>repeated bytes rawEntries = 4;</code>
   * @return A list containing the rawEntries.
   */
  java.util.List<com.google.protobuf.ByteString> getRawEntriesList();
  /**
   * <code>repeated bytes rawEntries = 4;</code>
   * @return The count of rawEntries.
   */
  int getRawEntriesCount();
  /**
   * <code>repeated bytes rawEntries = 4;</code>
   * @param index The index of the element to return.
   * @return The rawEntries at the given index.
   */
  com.google.protobuf.ByteString getRawEntries(int index);
}