package org.apache.pinot.broker.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.VirtualInstanceSelector;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.commons.lang3.tuple.Pair;



public class VirtualTableRoutingEntry implements RoutingEntry {
  private final List<RoutingEntry> _tableRoutingEntries;
  private final String _tableNameWithType;
  private final VirtualInstanceSelector _instanceSelector;

  VirtualTableRoutingEntry(String tableNameWithType, List<RoutingEntry> tableRoutingEntries) {
    _tableNameWithType = tableNameWithType;
    _tableRoutingEntries = tableRoutingEntries;
    _instanceSelector = new VirtualInstanceSelector();
    _instanceSelector.init(tableRoutingEntries.stream().collect(Collectors.toMap(RoutingEntry::getTableNameWithType, RoutingEntry::getInstanceSelector)));
  }

  @Override
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  @Override
  public int getLastUpdateIdealStateVersion() {
    return 0;
  }

  @Override
  public int getLastUpdateExternalViewVersion() {
    return 0;
  }

  @Override
  public void setTimeBoundaryManager(@Nullable TimeBoundaryManager timeBoundaryManager) {

  }

  @Nullable
  @Override
  public TimeBoundaryManager getTimeBoundaryManager() {
    return null;
  }

  @Nullable
  @Override
  public SegmentPartitionMetadataManager getPartitionMetadataManager() {
    return null;
  }

  @Override
  public Long getQueryTimeoutMs() {
    return null;
  }

  @Override
  public boolean isDisabled() {
    return false;
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView) {

  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {

  }

  @Override
  public void refreshSegment(String segment) {

  }

  @Override
  public InstanceSelector.SelectionResult calculateRouting(BrokerRequest brokerRequest, long requestId) {
    // Union the selection results
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>();
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    List<String> unavailableSegments = new ArrayList<>();
    int numPrunedSegments = 0;

    // Get all selection results
    for (RoutingEntry routingEntry : _tableRoutingEntries) {
      InstanceSelector.SelectionResult selectionResult = routingEntry.calculateRouting(brokerRequest, requestId);
      numPrunedSegments += selectionResult.getNumPrunedSegments();
      unavailableSegments.addAll(selectionResult.getUnavailableSegments());
      segmentToSelectedInstanceMap.putAll(selectionResult.getSegmentToInstanceMap());
      optionalSegmentToInstanceMap.putAll(selectionResult.getOptionalSegmentToInstanceMap());
    }

    return new InstanceSelector.SelectionResult(Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap), unavailableSegments, numPrunedSegments);
  }

  @Override
  public String getIdealStatePath() {
    return null;
  }

  @Override
  public String getExternalViewPath() {
    return null;
  }

  @Override
  public InstanceSelector getInstanceSelector() {
    return _instanceSelector;
  }

  @Override
  public boolean isVirtual() {
    return true;
  }
}
