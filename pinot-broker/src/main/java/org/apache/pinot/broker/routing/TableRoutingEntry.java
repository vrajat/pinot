package org.apache.pinot.broker.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.request.BrokerRequest;


class TableRoutingEntry implements RoutingEntry {
  final String _tableNameWithType;
  final String _idealStatePath;
  final String _externalViewPath;
  final SegmentPreSelector _segmentPreSelector;
  final SegmentSelector _segmentSelector;
  final List<SegmentPruner> _segmentPruners;
  final SegmentPartitionMetadataManager _partitionMetadataManager;
  final InstanceSelector _instanceSelector;
  final Long _queryTimeoutMs;
  final SegmentZkMetadataFetcher _segmentZkMetadataFetcher;

  // Cache IdealState and ExternalView version for the last update
  transient int _lastUpdateIdealStateVersion;
  transient int _lastUpdateExternalViewVersion;
  // Time boundary manager is only available for the offline part of the hybrid table
  transient TimeBoundaryManager _timeBoundaryManager;

  transient boolean _disabled;

  TableRoutingEntry(String tableNameWithType, String idealStatePath, String externalViewPath,
      SegmentPreSelector segmentPreSelector, SegmentSelector segmentSelector, List<SegmentPruner> segmentPruners,
      InstanceSelector instanceSelector, int lastUpdateIdealStateVersion, int lastUpdateExternalViewVersion,
      SegmentZkMetadataFetcher segmentZkMetadataFetcher, @Nullable TimeBoundaryManager timeBoundaryManager,
      @Nullable SegmentPartitionMetadataManager partitionMetadataManager, @Nullable Long queryTimeoutMs,
      boolean disabled) {
    _tableNameWithType = tableNameWithType;
    _idealStatePath = idealStatePath;
    _externalViewPath = externalViewPath;
    _segmentPreSelector = segmentPreSelector;
    _segmentSelector = segmentSelector;
    _segmentPruners = segmentPruners;
    _instanceSelector = instanceSelector;
    _lastUpdateIdealStateVersion = lastUpdateIdealStateVersion;
    _lastUpdateExternalViewVersion = lastUpdateExternalViewVersion;
    _timeBoundaryManager = timeBoundaryManager;
    _partitionMetadataManager = partitionMetadataManager;
    _queryTimeoutMs = queryTimeoutMs;
    _segmentZkMetadataFetcher = segmentZkMetadataFetcher;
    _disabled = disabled;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public int getLastUpdateIdealStateVersion() {
    return _lastUpdateIdealStateVersion;
  }

  public int getLastUpdateExternalViewVersion() {
    return _lastUpdateExternalViewVersion;
  }

  public void setTimeBoundaryManager(@Nullable TimeBoundaryManager timeBoundaryManager) {
    _timeBoundaryManager = timeBoundaryManager;
  }

  @Nullable
  public TimeBoundaryManager getTimeBoundaryManager() {
    return _timeBoundaryManager;
  }

  @Nullable
  public SegmentPartitionMetadataManager getPartitionMetadataManager() {
    return _partitionMetadataManager;
  }

  public Long getQueryTimeoutMs() {
    return _queryTimeoutMs;
  }

  public boolean isDisabled() {
    return _disabled;
  }

  // NOTE: The change gets applied in sequence, and before change applied to all components, there could be some
  // inconsistency between components, which is fine because the inconsistency only exists for the newly changed
  // segments and only lasts for a very short time.
  public void onAssignmentChange(IdealState idealState, ExternalView externalView) {
    Set<String> onlineSegments = BrokerRoutingManager.getOnlineSegments(idealState);
    Set<String> preSelectedOnlineSegments = _segmentPreSelector.preSelect(onlineSegments);
    _segmentZkMetadataFetcher.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
    _segmentSelector.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
    _instanceSelector.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
    if (_timeBoundaryManager != null) {
      _timeBoundaryManager.onAssignmentChange(idealState, externalView, preSelectedOnlineSegments);
    }
    _lastUpdateIdealStateVersion = idealState.getStat().getVersion();
    _lastUpdateExternalViewVersion = externalView.getStat().getVersion();
    _disabled = !idealState.isEnabled();
  }

  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _instanceSelector.onInstancesChange(enabledInstances, changedInstances);
  }

  public void refreshSegment(String segment) {
    _segmentZkMetadataFetcher.refreshSegment(segment);
    if (_timeBoundaryManager != null) {
      _timeBoundaryManager.refreshSegment(segment);
    }
  }

  public InstanceSelector.SelectionResult calculateRouting(BrokerRequest brokerRequest, long requestId) {
    Set<String> selectedSegments = _segmentSelector.select(brokerRequest);
    int numTotalSelectedSegments = selectedSegments.size();
    if (!selectedSegments.isEmpty()) {
      for (SegmentPruner segmentPruner : _segmentPruners) {
        selectedSegments = segmentPruner.prune(brokerRequest, selectedSegments);
      }
    }
    int numPrunedSegments = numTotalSelectedSegments - selectedSegments.size();
    if (!selectedSegments.isEmpty()) {
      InstanceSelector.SelectionResult selectionResult =
          _instanceSelector.select(brokerRequest, new ArrayList<>(selectedSegments), requestId);
      selectionResult.setNumPrunedSegments(numPrunedSegments);
      return selectionResult;
    } else {
      return new InstanceSelector.SelectionResult(Pair.of(Collections.emptyMap(), Collections.emptyMap()),
          Collections.emptyList(), numPrunedSegments);
    }
  }

  @Override
  public String getIdealStatePath() {
    return _idealStatePath;
  }

  @Override
  public String getExternalViewPath() {
    return _externalViewPath;
  }

  @Override
  public InstanceSelector getInstanceSelector() {
    return _instanceSelector;
  }

  @Override
  public boolean isVirtual() {
    return false;
  }
}
