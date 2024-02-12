package org.apache.pinot.controller.helix.core.restream;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;

public class ZkBasedTableRestreamObserver implements TableRestreamObserver {
  private final String _tableNameWithType;
  private final String _rebalanceJobId;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final TableRestreamContext _tableRestreamContext;
  private int _numUpdatesToZk;
  private final ControllerMetrics _controllerMetrics;

  public ZkBasedTableRestreamObserver(String tableNameWithType, String restreamJobId,
      TableRestreamContext tableRestreamContext, PinotHelixResourceManager pinotHelixResourceManager) {
    Preconditions.checkState(tableNameWithType != null, "Table name cannot be null");
    Preconditions.checkState(restreamJobId != null, "rebalanceId cannot be null");
    Preconditions.checkState(pinotHelixResourceManager != null, "PinotHelixManager cannot be null");
    _tableNameWithType = tableNameWithType;
    _rebalanceJobId = restreamJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tableRestreamContext = tableRestreamContext;
    _numUpdatesToZk = 0;
    _controllerMetrics = ControllerMetrics.get();
  }
}
