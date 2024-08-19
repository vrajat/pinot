package org.apache.pinot.common.response;

import java.util.List;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;


public interface CursorResponse {

  /**
   * Get the requestId of the query
   * @return requestId of the query
   */
  String getRequestId();

  /**
   * get broker ID of the processing broker
   */
  String getBrokerId();

  /**
   * get hostname of the processing broker
   * @return String containing the hostname
   */
  String getBrokerHost();

  /**
   * get port of the processing broker
   * @return int containing the port.
   */
  int getBrokerPort();

  /**
   * Get the result table.
   * @return result table.
   */
  ResultTable getResultTable();

  /**
   * Get the total number of rows in result set
   */
  int getNumRowsResultSet();

  /**
   * Current offset in the query result.
   * Starts from 0.
   * @return current offset.
   */
  int getOffset();

  /**
   * Number of rows in the current response.
   * @return Number of rows in the current response.
   */
  int getNumRows();

  /**
   * Query Params to get the next result set.
   * The next response will have the same number of rows as the current response.
   * @return Query Params to get the next result set.
   */
  String getNextOffsetParams();

  /**
   * Gets a list of processing exceptions.
   * @return A list of processing exceptions.
   */
  List<QueryProcessingException> getProcessingExceptions();

  /**
   * Return the time to write the results to the query store.
   * @return time in milliseconds
   */
  long getCursorResultWriteTimeMs();

  /**
   * Time taken to write cursor results to query storage.
   * @param cursorResultWriteMs Time in milliseconds.
   */
  void setCursorResultWriteTimeMs(long cursorResultWriteMs);

  /**
   * Return the time to fetch results from the query store.
   * @return time in milliseconds.
   */
  long getCursorFetchTimeMs();
}
