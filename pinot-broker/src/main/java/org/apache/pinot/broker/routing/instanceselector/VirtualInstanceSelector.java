package org.apache.pinot.broker.routing.instanceselector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.request.BrokerRequest;


public class VirtualInstanceSelector implements InstanceSelector {
  Map<String, InstanceSelector> _tableNameInstanceSelector = new HashMap<>();

  public void init(Map<String, InstanceSelector> instanceSelectors) {
    _tableNameInstanceSelector = instanceSelectors;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {

  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {

  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {

  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    return null;
  }

  @Override
  public Set<String> getServingInstances() {
    Set<String> servingInstances = new HashSet<>();
    for (InstanceSelector instanceSelector : _tableNameInstanceSelector.values()) {
      servingInstances.addAll(instanceSelector.getServingInstances());
    }

    return servingInstances;
  }
}
