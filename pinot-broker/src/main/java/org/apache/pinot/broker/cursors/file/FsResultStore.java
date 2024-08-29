package org.apache.pinot.broker.cursors.file;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.spi.cursors.QueryStore;
import org.apache.pinot.spi.cursors.ResultMetadata;
import org.apache.pinot.spi.cursors.ResultStore;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FsResultStore implements ResultStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FsResultStore.class);
  private static final String RESULT_TABLE_FILE_NAME_FORMAT = "resultTable.json";
  private static final String RESPONSE_FILE_NAME_FORMAT = "response.json";

  Path _localTempDir;
  URI _dataDir;

  @Override
  public void init(PinotConfiguration config)
      throws Exception {
    _localTempDir = Paths.get(config.getProperty(CommonConstants.CursorConfigs.TEMP_DIR,
        CommonConstants.CursorConfigs.DEFAULT_TEMP_DIR));
    Files.createDirectories(_localTempDir);

    Preconditions.checkNotNull(config.getProperty(CommonConstants.CursorConfigs.DATA_DIR));
    _dataDir = new URI(config.getProperty(CommonConstants.CursorConfigs.DATA_DIR));
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    pinotFS.mkdir(_dataDir);
  }

  @Override
  public QueryStore initQueryStore(ResultMetadata metadata)
      throws Exception {
    String requestId = metadata.getRequestId();
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = Utils.combinePath(_dataDir, requestId);

    // Create a directory for this query.
    pinotFS.mkdir(queryDir);
    FsMetadata fsMetadata = new FsMetadata(Utils.combinePath(queryDir, RESULT_TABLE_FILE_NAME_FORMAT),
        Utils.combinePath(queryDir, RESPONSE_FILE_NAME_FORMAT), metadata);
    FsMetadata.write(pinotFS, Utils.getTempPath(_localTempDir, requestId, FsQueryStore.METADATA_FILENAME),
        FsQueryStore.getMetadataFile(queryDir), fsMetadata);
    return new FsQueryStore(pinotFS, requestId, _localTempDir, queryDir);
  }

  @Override
  public QueryStore getQueryStore(String requestId)
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    URI queryDir = Utils.combinePath(_dataDir, requestId);

    if (pinotFS.exists(queryDir)) {
      return new FsQueryStore(pinotFS, requestId, _localTempDir, queryDir);
    }

    return null;
  }

  @Override
  public Collection<? extends QueryStore> getAllQueryStores()
      throws Exception {
    PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
    List<FileMetadata> queryPaths = pinotFS.listFilesWithMetadata(_dataDir, true);
    List<QueryStore> queryStoreList = new ArrayList<>(queryPaths.size());

    LOGGER.debug(String.format("Found %d paths.", queryPaths.size()));

    for (FileMetadata metadata : queryPaths) {
      LOGGER.debug(String.format("Processing query path: %s", metadata.toString()));
      if (metadata.isDirectory()) {
        try {
          URI queryDir = new URI(metadata.getFilePath());
          URI metadataFile = FsQueryStore.getMetadataFile(queryDir);
          boolean metadataFileExists = pinotFS.exists(metadataFile);
          LOGGER.debug(
              String.format("Checking for query dir %s & metadata file: %s. Metadata file exists: %s", queryDir,
                  metadataFile, metadataFileExists));
          if (metadataFileExists) {
            FsMetadata fsMetadata = FsMetadata.read(pinotFS, metadataFile);
            ResultMetadata resultMetadata = fsMetadata.getResultMetadata();
            queryStoreList.add(new FsQueryStore(pinotFS, resultMetadata.getRequestId(), _localTempDir, queryDir));
            LOGGER.debug("Added query store {}", queryDir);
          }
        } catch (Exception e) {
          LOGGER.error("Error when processing {}", metadata, e);
        }
      }
    }

    return queryStoreList;
  }

  @Override
  public QueryStore deleteQueryStore(String requestId)
      throws Exception {
    QueryStore queryStore = getQueryStore(requestId);
    if (queryStore != null) {
      PinotFS pinotFS = PinotFSFactory.create(_dataDir.getScheme());
      URI queryDir = Utils.combinePath(_dataDir, requestId);
      pinotFS.delete(queryDir, true);
    }

    return queryStore;
  }
}
