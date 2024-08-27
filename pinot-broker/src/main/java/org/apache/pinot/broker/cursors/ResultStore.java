package org.apache.pinot.broker.cursors;

import java.util.Collection;
import org.apache.pinot.common.cursors.ResultMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * ResultStore is a collection of Query Stores.
 * It maintains metadata for all the query stores in a broker.
 * <br/>
 * Concurrency Model:
 * <br/>
 * There are 3 possible roles - writer, reader and delete.
 * <br/>
 * There can only be ONE writer and no other concurrent roles can execute.
 * A query store is written during query execution. During execution, there can be no reads or deletes as the
 * query id would not have been provided to the client.
 * <br/>
 * There can be multiple readers. There maybe concurrent deletes but no concurrent writes.
 * Multiple clients can potentially iterate through the result set.
 * <br/>
 * There can be multiple deletes. There maybe concurrent reads but no concurrent writes.
 * Multiple clients can potentially call the delete API.
 * <br/>
 * Implementations should ensure that concurrent read/delete and delete/delete operations are handled correctly.
 */
public interface ResultStore {
  /**
   * Initialize the store.
   * @param config Configuration of the store.
   */
  void init(PinotConfiguration config)
      throws Exception;

  /**
   * Initialize and store a QueryStore for a specific query request ID.
   * @param metadata  ResultMetadata object that captures all the information about the query request and results.
   */
  QueryStore initQueryStore(ResultMetadata metadata)
      throws Exception;

  /**
   * Get a QueryStore for a specific query
   * @param requestId Query ID of the query
   * @return A QueryStore object.
   * @throws Exception
   */
  QueryStore getQueryStore(String requestId)
      throws Exception;

  /**
   * Get All Query Stores.
   *
   * @return List of QueryStore objects
   */
  Collection<? extends QueryStore> getAllQueryStores()
      throws Exception;

  /**
   * Delete a QueryStore for a query id.
   *
   * @param requestId Query Id of the query.
   * @return The deleted QueryStore
   * @throws Exception
   */
  QueryStore deleteQueryStore(String requestId)
      throws Exception;
}
