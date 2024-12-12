package org.apache.pinot.core.data.manager;

import com.google.common.cache.Cache;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.StaleSegment;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class VirtualTableDataManager implements TableDataManager {
  List<TableDataManager> _physicalTableDataManagers;

  @Override
  public void init(InstanceDataManagerConfig instanceDataManagerConfig, HelixManager helixManager,
      SegmentLocks segmentLocks, TableConfig tableConfig, @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable Cache<Pair<String, String>, SegmentErrorInfo> errorCache) {

  }

  public void init(List<TableDataManager> physicalTableDataManagers) {
    _physicalTableDataManagers = physicalTableDataManagers;
  }

  @Override
  public String getInstanceId() {
    return null;
  }

  @Override
  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return null;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutDown() {

  }

  @Override
  public boolean isShutDown() {
    return false;
  }

  @Override
  public Lock getSegmentLock(String segmentName) {
    return null;
  }

  @Override
  public boolean hasSegment(String segmentName) {
    return
        _physicalTableDataManagers.stream().map(p -> p.hasSegment(segmentName)).reduce(false, (a, b) -> a || b);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    throw new NotImplementedException();
  }

  @Override
  public void addOnlineSegment(String segmentName)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void addNewOnlineSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public boolean tryLoadExistingSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    return false;
  }

  @Override
  public boolean needReloadSegments()
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void downloadAndLoadSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void addConsumingSegment(String segmentName)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void replaceSegment(String segmentName)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void offloadSegment(String segmentName)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void offloadSegmentUnsafe(String segmentName)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig) {
    return null;
  }

  @Override
  public boolean isSegmentDeletedRecently(String segmentName) {
    return false;
  }

  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    return null;
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments) {
    List<SegmentDataManager> managerList = new ArrayList<>(segmentNames.size());
    for (TableDataManager dataManager : _physicalTableDataManagers) {
      List<String> segmentsOfTable = new ArrayList<>(segmentNames.size());
      for (String segment: segmentNames) {
        if (dataManager.hasSegment(segment)) {
          segmentsOfTable.add(segment);
        }
      }
      managerList.addAll(dataManager.acquireSegments(segmentsOfTable, missingSegments));
    }

    return managerList;
  }

  @Nullable
  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    return null;
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {

  }

  @Override
  public int getNumSegments() {
    return 0;
  }

  @Override
  public String getTableName() {
    return null;
  }

  @Override
  public File getTableDataDir() {
    return null;
  }

  @Override
  public HelixManager getHelixManager() {
    return null;
  }

  @Override
  public ExecutorService getSegmentPreloadExecutor() {
    return null;
  }

  @Override
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {

  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    return null;
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    List<SegmentContext> segmentContexts = new ArrayList<>(selectedSegments.size());
    selectedSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    return segmentContexts;
  }

  @Override
  public SegmentZKMetadata fetchZKMetadata(String segmentName) {
    return null;
  }

  @Override
  public Pair<TableConfig, Schema> fetchTableConfigAndSchema() {
    return null;
  }

  @Override
  public IndexLoadingConfig getIndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    throw new NotImplementedException();
  }

  @Override
  public List<StaleSegment> getStaleSegments(TableConfig tableConfig, Schema schema) {
    throw new NotImplementedException();
  }
}
