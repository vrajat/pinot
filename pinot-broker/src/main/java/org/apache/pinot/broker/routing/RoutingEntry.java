package org.apache.pinot.broker.routing;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.request.BrokerRequest;


public interface RoutingEntry {
  String getTableNameWithType();

  int getLastUpdateIdealStateVersion();

  int getLastUpdateExternalViewVersion();

  void setTimeBoundaryManager(@Nullable TimeBoundaryManager timeBoundaryManager);

  @Nullable
  TimeBoundaryManager getTimeBoundaryManager();

  @Nullable
  SegmentPartitionMetadataManager getPartitionMetadataManager();

  Long getQueryTimeoutMs();

  boolean isDisabled();

  // NOTE: The change gets applied in sequence, and before change applied to all components, there could be some
  // inconsistency between components, which is fine because the inconsistency only exists for the newly changed
  // segments and only lasts for a very short time.
  void onAssignmentChange(IdealState idealState, ExternalView externalView);

  void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances);

  void refreshSegment(String segment);

  InstanceSelector.SelectionResult calculateRouting(BrokerRequest brokerRequest, long requestId);

  boolean isVirtual();
  String getIdealStatePath();
  String getExternalViewPath();
  InstanceSelector getInstanceSelector();
}
