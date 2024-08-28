package org.apache.pinot.spi.cursors;

import org.apache.pinot.spi.query.QueryResponse;


public interface CursorResponse extends QueryResponse {

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
