/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.requesthandler;

import java.util.Map;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.trace.RequestContext;


public class QueryRouteInfo {
  public final static QueryRouteInfo EMPTY =
      new QueryRouteInfo(-1, null, null, null, null, null, null, null, null, null, 0, -1, null);

  private final long _requestId;
  private final String _rawTableName;
  private BrokerRequest _originalBrokerRequest;
  private final BrokerRequest _serverBrokerRequest;
  private final String _offlineTableName;
  private final BrokerRequest _offlineBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _offlineRoutingTable;
  private final String _realtimeTableName;
  private final BrokerRequest _realtimeBrokerRequest;
  private final Map<ServerInstance, ServerRouteInfo> _realtimeRoutingTable;
  private final int numPrunedSegments;
  private final long _timeoutMs;
  private final RequestContext _requestContext;

  public QueryRouteInfo(long requestId, String rawTableName, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, String offlineTableName, BrokerRequest offlineBrokerRequest,
      Map<ServerInstance, ServerRouteInfo> offlineRoutingTable, String realtimeTableName,
      BrokerRequest realtimeBrokerRequest, Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable,
      int numPrunedSegments, long timeoutMs, RequestContext requestContext) {
    this._requestId = requestId;
    _rawTableName = rawTableName;
    this._originalBrokerRequest = originalBrokerRequest;
    this._serverBrokerRequest = serverBrokerRequest;
    this._offlineTableName = offlineTableName;
    this._offlineBrokerRequest = offlineBrokerRequest;
    this._offlineRoutingTable = offlineRoutingTable;
    this._realtimeTableName = realtimeTableName;
    this._realtimeBrokerRequest = realtimeBrokerRequest;
    this._realtimeRoutingTable = realtimeRoutingTable;
    this.numPrunedSegments = numPrunedSegments;
    this._timeoutMs = timeoutMs;
    this._requestContext = requestContext;
  }

  // Getters and setters for each member variable
  public long getRequestId() {
    return _requestId;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public BrokerRequest getOriginalBrokerRequest() {
    return _originalBrokerRequest;
  }

  public void setOriginalBrokerRequest(BrokerRequest originalBrokerRequest) {
    this._originalBrokerRequest = originalBrokerRequest;
  }

  public BrokerRequest getServerBrokerRequest() {
    return _serverBrokerRequest;
  }

  public String getOfflineTableName() {
    return _offlineTableName;
  }

  public BrokerRequest getOfflineBrokerRequest() {
    return _offlineBrokerRequest;
  }

  public Map<ServerInstance, ServerRouteInfo> getOfflineRoutingTable() {
    return _offlineRoutingTable;
  }

  public String getRealtimeTableName() {
    return _realtimeTableName;
  }

  public BrokerRequest getRealtimeBrokerRequest() {
    return _realtimeBrokerRequest;
  }

  public Map<ServerInstance, ServerRouteInfo> getRealtimeRoutingTable() {
    return _realtimeRoutingTable;
  }

  public int getNumPrunedSegments() {
    return numPrunedSegments;
  }

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  public RequestContext getRequestContext() {
    return _requestContext;
  }
}