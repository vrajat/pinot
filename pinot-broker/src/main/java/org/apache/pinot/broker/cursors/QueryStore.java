package org.apache.pinot.broker.cursors;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.cursors.ResultMetadata;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.ResultTable;


/**
 * A query store represents storage for a specific query.
 * For example a specific query's storage maybe a directory in a file system or
 * the value in a map.
 */
public interface QueryStore {
  /**
   * Add a response for a query.
   * A query's result set maybe made up of many BrokerResponses. Add a response to the list of responses of the query.
   * It is assumed that the responses are added in the sequence it should be returned.
   * The implementation is expected to also manage any metadata associated with the response.
   * @param response BrokerResponse containing a partial result set.
   * @throws Exception
   */
  void addResponse(BrokerResponse response)
      throws Exception;

  /**
   * Get a response that contains a specific offset.
   * @param offset Global offset in the complete result set of the query.
   * @param numRows Number of rows to return.
   * @return A BrokerResponse that contains the offset.
   * @throws Exception
   */
  CursorResponse getResponse(int offset, int numRows)
      throws Exception;

  /**
   * Return metadata about the query result in QueryStore object
   * @return ResultMetadata object
   */
  ResultMetadata getResultMetadata()
      throws Exception;

  /**
   * Get the result table at a specific index
   * @param index Index in the list of ResultTable objects
   * @return Returns the object at an index
   * @throws IndexOutOfBoundsException if index is out of bounds of the list.
   */
  ResultTable getResultTable(int index)
      throws Exception;

  /**
   * Get all BrokerResponse objects without the ResultTable.
   * Note that JsonNode objects are returned since serialized versions do not have type information to
   * instantiate an implementation of BrokerResponse interface.
   * @return List of JsonNode objects of BrokerResponse objects
   */
  List<JsonNode> getBrokerResponses()
      throws IOException;
}
