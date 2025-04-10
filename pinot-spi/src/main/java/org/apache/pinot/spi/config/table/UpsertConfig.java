/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


/** Class representing upsert configuration of a table. */
public class UpsertConfig extends BaseJsonConfig {

  public enum Mode {
    FULL, PARTIAL, NONE
  }

  public enum Strategy {
    // Todo: add CUSTOM strategies
    APPEND, IGNORE, INCREMENT, MAX, MIN, OVERWRITE, FORCE_OVERWRITE, UNION
  }

  public enum ConsistencyMode {
    NONE, SYNC, SNAPSHOT
  }

  @JsonPropertyDescription("Upsert mode.")
  private Mode _mode;

  @JsonPropertyDescription("Function to hash the primary key.")
  private HashFunction _hashFunction = HashFunction.NONE;

  @JsonPropertyDescription("Partial update strategies.")
  private Map<String, Strategy> _partialUpsertStrategies;

  @JsonPropertyDescription("default upsert strategy for partial mode")
  private Strategy _defaultPartialUpsertStrategy = Strategy.OVERWRITE;

  @JsonPropertyDescription("Class name for custom row merger implementation")
  private String _partialUpsertMergerClass;

  @JsonPropertyDescription("Columns for upsert comparison, default to time column")
  private List<String> _comparisonColumns;

  @JsonPropertyDescription("Boolean column to indicate whether a records should be deleted")
  private String _deleteRecordColumn;

  @JsonPropertyDescription("Boolean column to indicate whether a records is out-of-order")
  private String _outOfOrderRecordColumn;

  @JsonPropertyDescription("Whether to use snapshot for fast upsert metadata recovery")
  private boolean _enableSnapshot;

  @JsonPropertyDescription("Whether to use TTL for upsert metadata cleanup, it uses the same unit as comparison col")
  private double _metadataTTL;

  @JsonPropertyDescription("TTL for upsert metadata cleanup for deleted keys, it uses the same unit as comparison col")
  private double _deletedKeysTTL;

  @JsonPropertyDescription("If we are using deletionKeysTTL + compaction we need to enable this for data consistency")
  private boolean _enableDeletedKeysCompactionConsistency;

  @JsonPropertyDescription("Whether to preload segments for fast upsert metadata recovery")
  private boolean _enablePreload;

  @JsonPropertyDescription("Configure the way to provide consistent view for upsert table")
  private ConsistencyMode _consistencyMode = ConsistencyMode.NONE;

  @JsonPropertyDescription("Refresh interval when using the snapshot consistency mode")
  private long _upsertViewRefreshIntervalMs = 3000;

  // Setting this time to 0 to disable the tracking feature.
  @JsonPropertyDescription("Track newly added segments on the server for a more complete upsert data view.")
  private long _newSegmentTrackingTimeMs = 10000;

  @JsonPropertyDescription("Custom class for upsert metadata manager")
  private String _metadataManagerClass;

  @JsonPropertyDescription("Custom configs for upsert metadata manager")
  private Map<String, String> _metadataManagerConfigs;

  @JsonPropertyDescription("Whether to drop out-of-order record")
  private boolean _dropOutOfOrderRecord;

  /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
  @Deprecated
  @JsonPropertyDescription("Whether to pause partial upsert table's partition consumption during commit")
  private boolean _allowPartialUpsertConsumptionDuringCommit;

  public UpsertConfig(Mode mode) {
    _mode = mode;
  }

  // Do not use this constructor. This is needed for JSON deserialization.
  public UpsertConfig() {
  }

  public Mode getMode() {
    return _mode;
  }

  public void setMode(Mode mode) {
    _mode = mode;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  @Nullable
  public Map<String, Strategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }

  public Strategy getDefaultPartialUpsertStrategy() {
    return _defaultPartialUpsertStrategy;
  }

  public String getPartialUpsertMergerClass() {
    return _partialUpsertMergerClass;
  }

  public List<String> getComparisonColumns() {
    return _comparisonColumns;
  }

  @Nullable
  public String getDeleteRecordColumn() {
    return _deleteRecordColumn;
  }

  @Nullable
  public String getOutOfOrderRecordColumn() {
    return _outOfOrderRecordColumn;
  }

  public boolean isEnableSnapshot() {
    return _enableSnapshot;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public double getDeletedKeysTTL() {
    return _deletedKeysTTL;
  }

  public boolean isEnablePreload() {
    return _enablePreload;
  }

  public boolean isEnableDeletedKeysCompactionConsistency() {
    return _enableDeletedKeysCompactionConsistency;
  }

  public ConsistencyMode getConsistencyMode() {
    return _consistencyMode;
  }

  public long getUpsertViewRefreshIntervalMs() {
    return _upsertViewRefreshIntervalMs;
  }

  public long getNewSegmentTrackingTimeMs() {
    return _newSegmentTrackingTimeMs;
  }

  public boolean isDropOutOfOrderRecord() {
    return _dropOutOfOrderRecord;
  }

  @Nullable
  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  public void setHashFunction(HashFunction hashFunction) {
    _hashFunction = hashFunction;
  }

  /**
   * PartialUpsertStrategies maintains the mapping of merge strategies per column.
   * Each key in the map is a columnName, value is a partial upsert merging strategy.
   * Supported strategies are {OVERWRITE|INCREMENT|APPEND|UNION|IGNORE}.
   */
  public void setPartialUpsertStrategies(Map<String, Strategy> partialUpsertStrategies) {
    _partialUpsertStrategies = partialUpsertStrategies;
  }

  /**
   * If strategy is not specified for a column, the merger on that column will be "defaultPartialUpsertStrategy".
   * The default value of defaultPartialUpsertStrategy is OVERWRITE.
   */
  public void setDefaultPartialUpsertStrategy(Strategy defaultPartialUpsertStrategy) {
    _defaultPartialUpsertStrategy = defaultPartialUpsertStrategy;
  }

  /**
   * Specify to plug a custom implementation for merging rows in partial upsert realtime table.
   * @param partialUpsertMergerClass
   */
  public void setPartialUpsertMergerClass(String partialUpsertMergerClass) {
    _partialUpsertMergerClass = partialUpsertMergerClass;
  }

  /**
   * By default, Pinot uses the value in the time column to determine the latest record. For two records with the
   * same primary key, the record with the larger value of the time column is picked as the
   * latest update.
   * However, there are cases when users need to use another column to determine the order.
   * In such case, you can use option comparisonColumn to override the column used for comparison. When using
   * multiple comparison columns, typically in the case of partial upserts, it is expected that input documents will
   * each only have a singular non-null comparisonColumn. Multiple non-null values in an input document _will_ result
   * in undefined behaviour. Typically, one comparisonColumn is allocated per distinct producer application of data
   * in the case where there are multiple producers sinking to the same table.
   */
  public void setComparisonColumns(List<String> comparisonColumns) {
    if (CollectionUtils.isNotEmpty(comparisonColumns)) {
      _comparisonColumns = comparisonColumns;
    }
  }

  public void setComparisonColumn(String comparisonColumn) {
    if (comparisonColumn != null) {
      _comparisonColumns = Collections.singletonList(comparisonColumn);
    }
  }

  public void setDeleteRecordColumn(String deleteRecordColumn) {
    _deleteRecordColumn = deleteRecordColumn;
  }

  public void setOutOfOrderRecordColumn(String outOfOrderRecordColumn) {
    _outOfOrderRecordColumn = outOfOrderRecordColumn;
  }

  public void setEnableSnapshot(boolean enableSnapshot) {
    _enableSnapshot = enableSnapshot;
  }

  public void setMetadataTTL(double metadataTTL) {
    _metadataTTL = metadataTTL;
  }

  public void setDeletedKeysTTL(double deletedKeysTTL) {
    _deletedKeysTTL = deletedKeysTTL;
  }

  public void setEnablePreload(boolean enablePreload) {
    _enablePreload = enablePreload;
  }

  public void setConsistencyMode(ConsistencyMode consistencyMode) {
    _consistencyMode = consistencyMode;
  }

  public void setUpsertViewRefreshIntervalMs(long upsertViewRefreshIntervalMs) {
    _upsertViewRefreshIntervalMs = upsertViewRefreshIntervalMs;
  }

  public void setNewSegmentTrackingTimeMs(long newSegmentTrackingTimeMs) {
    _newSegmentTrackingTimeMs = newSegmentTrackingTimeMs;
  }

  public void setDropOutOfOrderRecord(boolean dropOutOfOrderRecord) {
    _dropOutOfOrderRecord = dropOutOfOrderRecord;
  }

  public void setEnableDeletedKeysCompactionConsistency(boolean enableDeletedKeysCompactionConsistency) {
    _enableDeletedKeysCompactionConsistency = enableDeletedKeysCompactionConsistency;
  }

  public void setMetadataManagerClass(String metadataManagerClass) {
    _metadataManagerClass = metadataManagerClass;
  }

  public void setMetadataManagerConfigs(Map<String, String> metadataManagerConfigs) {
    _metadataManagerConfigs = metadataManagerConfigs;
  }

  @Deprecated
  public void setAllowPartialUpsertConsumptionDuringCommit(boolean allowPartialUpsertConsumptionDuringCommit) {
    _allowPartialUpsertConsumptionDuringCommit = allowPartialUpsertConsumptionDuringCommit;
  }

  @Deprecated
  public boolean isAllowPartialUpsertConsumptionDuringCommit() {
    return _allowPartialUpsertConsumptionDuringCommit;
  }
}
