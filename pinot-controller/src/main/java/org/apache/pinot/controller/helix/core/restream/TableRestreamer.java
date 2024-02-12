package org.apache.pinot.controller.helix.core.restream;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.spi.config.table.TableConfig;


public class TableRestreamer {
  private final HelixManager _helixManager;
  private final HelixDataAccessor _helixDataAccessor;
  private final TableRestreamObserver _tableRestreamObserver;
  private final ControllerMetrics _controllerMetrics;

  public TableRestreamer(HelixManager helixManager, @Nullable TableRestreamObserver tableRestreamObserver,
      @Nullable ControllerMetrics controllerMetrics) {
    _helixManager = helixManager;
    if (tableRestreamObserver != null) {
      _tableRestreamObserver = tableRestreamObserver;
    } else {
      _tableRestreamObserver = new NoOpTableRestreamObserver();
    }
    _helixDataAccessor = helixManager.getHelixDataAccessor();
    _controllerMetrics = controllerMetrics;
  }

  public TableRestreamer(HelixManager helixManager) {
    this(helixManager, null, null);
  }

  public static String createUniqueJobIdentifier() {
    return UUID.randomUUID().toString();
  }

  public TableRestreamResult restream(TableConfig tableConfig, TableRestreamConfig tableRestreamConfig,
      @Nullable String restreamJobId) {
    long startTime = System.currentTimeMillis();
    String tableNameWithType = tableConfig.getTableName();
    TableRestreamResult.Status status = TableRestreamResult.Status.UNKNOWN_ERROR;
    try {
      TableRestreamResult result = doRestream(tableConfig, tableRestreamConfig, restreamJobId);
      status = result.getStatus();
      return result;
    } finally {
      if (_controllerMetrics != null) {
        _controllerMetrics.addTimedTableValue(String.format("%s.%s", tableNameWithType, status.toString()),
            ControllerTimer.TABLE_RESTREAM_EXECUTION_TIME_MS, System.currentTimeMillis() - startTime,
            TimeUnit.MILLISECONDS);
      }
    }
  }

  private TableRestreamResult doRestream(TableConfig tableConfig, TableRestreamConfig tableRestreamConfig,
      @Nullable String restreamJobId) {
    return new TableRestreamResult(restreamJobId, TableRestreamResult.Status.FAILED, TableRestreamResult.Stage.INIT,
        "Not Implemented");
  }
}
