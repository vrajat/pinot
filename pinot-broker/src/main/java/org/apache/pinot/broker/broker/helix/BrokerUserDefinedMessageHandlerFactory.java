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
package org.apache.pinot.broker.broker.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.messages.ApplicationQpsQuotaRefreshMessage;
import org.apache.pinot.common.messages.DatabaseConfigRefreshMessage;
import org.apache.pinot.common.messages.LogicalTableConfigRefreshMessage;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.common.messages.RoutingTableRebuildMessage;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.TableConfigRefreshMessage;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker message handler factory for Helix user-define messages.
 * <p>The following message sub-types are supported:
 * <ul>
 *   <li>Refresh segment message: Refresh the routing properties for a given segment</li>
 * </ul>
 */
public class BrokerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerUserDefinedMessageHandlerFactory.class);

  private final BrokerRoutingManager _routingManager;
  private final HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;

  public BrokerUserDefinedMessageHandlerFactory(BrokerRoutingManager routingManager,
      HelixExternalViewBasedQueryQuotaManager queryQuotaManager) {
    _routingManager = routingManager;
    _queryQuotaManager = queryQuotaManager;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String msgSubType = message.getMsgSubType();
    switch (msgSubType) {
      case SegmentRefreshMessage.REFRESH_SEGMENT_MSG_SUB_TYPE:
        return new RefreshSegmentMessageHandler(new SegmentRefreshMessage(message), context);
      case TableConfigRefreshMessage.REFRESH_TABLE_CONFIG_MSG_SUB_TYPE:
        return new RefreshTableConfigMessageHandler(new TableConfigRefreshMessage(message), context);
      case LogicalTableConfigRefreshMessage.REFRESH_LOGICAL_TABLE_CONFIG_MSG_SUB_TYPE:
        return new RefreshLogicalTableConfigMessageHandler(new LogicalTableConfigRefreshMessage(message), context);
      case RoutingTableRebuildMessage.REBUILD_ROUTING_TABLE_MSG_SUB_TYPE:
        return new RebuildRoutingTableMessageHandler(new RoutingTableRebuildMessage(message), context);
      case DatabaseConfigRefreshMessage.REFRESH_DATABASE_CONFIG_MSG_SUB_TYPE:
        return new RefreshDatabaseConfigMessageHandler(new DatabaseConfigRefreshMessage(message), context);
      case ApplicationQpsQuotaRefreshMessage.REFRESH_APP_QUOTA_MSG_SUB_TYPE:
        return new RefreshApplicationQpsQuotaMessageHandler(new ApplicationQpsQuotaRefreshMessage(message), context);
      case QueryWorkloadRefreshMessage.REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE:
      case QueryWorkloadRefreshMessage.DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE:
        return new QueryWorkloadRefreshMessageHandler(new QueryWorkloadRefreshMessage(message), context);
      default:
        // NOTE: Log a warning and return no-op message handler for unsupported message sub-types. This can happen when
        //       a new message sub-type is added, and the sender gets deployed first while receiver is still running the
        //       old version.
        LOGGER.warn("Received message with unsupported sub-type: {}, using no-op message handler", msgSubType);
        return new NoOpMessageHandler(message, context);
    }
  }

  @Override
  public String getMessageType() {
    return Message.MessageType.USER_DEFINE_MSG.toString();
  }

  @Override
  public void reset() {
  }

  private class RefreshSegmentMessageHandler extends MessageHandler {
    final String _tableNameWithType;
    final String _segmentName;

    RefreshSegmentMessageHandler(SegmentRefreshMessage segmentRefreshMessage, NotificationContext context) {
      super(segmentRefreshMessage, context);
      _tableNameWithType = segmentRefreshMessage.getTableNameWithType();
      _segmentName = segmentRefreshMessage.getSegmentName();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _routingManager.refreshSegment(_tableNameWithType, _segmentName);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing segment: {} of table: {} (error code: {}, error type: {})", _segmentName,
          _tableNameWithType, code, type, e);
    }
  }

  private class RefreshTableConfigMessageHandler extends MessageHandler {
    final String _tableNameWithType;

    RefreshTableConfigMessageHandler(TableConfigRefreshMessage tableConfigRefreshMessage, NotificationContext context) {
      super(tableConfigRefreshMessage, context);
      _tableNameWithType = tableConfigRefreshMessage.getTableNameWithType();
    }

    @Override
    public HelixTaskResult handleMessage() {
      // TODO: Fetch the table config here and pass it into the managers, or consider merging these 2 managers
      _routingManager.buildRouting(_tableNameWithType);
      _queryQuotaManager.initOrUpdateTableQueryQuota(_tableNameWithType);
      // only create the rate limiter if not present. This message has no reason to update the database rate limiter
      _queryQuotaManager.createDatabaseRateLimiter(
          DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(_tableNameWithType));
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing table config for table: {} (error code: {}, error type: {})",
          _tableNameWithType, code, type, e);
    }
  }

  private class RefreshLogicalTableConfigMessageHandler extends MessageHandler {
    final String _logicalTableName;

    RefreshLogicalTableConfigMessageHandler(LogicalTableConfigRefreshMessage refreshMessage,
        NotificationContext context) {
      super(refreshMessage, context);
      _logicalTableName = refreshMessage.getLogicalTableName();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _routingManager.buildRoutingForLogicalTable(_logicalTableName);
      _queryQuotaManager.initOrUpdateLogicalTableQueryQuota(_logicalTableName);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing logical table config for table: {} (error code: {}, error type: {})",
          _logicalTableName, code, type, e);
    }
  }

  private class RefreshDatabaseConfigMessageHandler extends MessageHandler {
    final String _databaseName;

    RefreshDatabaseConfigMessageHandler(DatabaseConfigRefreshMessage databaseConfigRefreshMessage,
        NotificationContext context) {
      super(databaseConfigRefreshMessage, context);
      _databaseName = databaseConfigRefreshMessage.getDatabaseName();
    }

    @Override
    public HelixTaskResult handleMessage() {
      // only update the existing rate limiter.
      // Database rate limiter creation should only be done through table based change triggers
      _queryQuotaManager.updateDatabaseRateLimiter(_databaseName);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing database config for database: {} (error code: {}, error type: {})",
          _databaseName, code, type, e);
    }
  }

  private class RefreshApplicationQpsQuotaMessageHandler extends MessageHandler {
    final String _applicationName;

    RefreshApplicationQpsQuotaMessageHandler(ApplicationQpsQuotaRefreshMessage applicationQpsAuotaRefreshMessage,
        NotificationContext context) {
      super(applicationQpsAuotaRefreshMessage, context);
      _applicationName = applicationQpsAuotaRefreshMessage.getApplicationName();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _queryQuotaManager.createOrUpdateApplicationRateLimiter(_applicationName);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing query quota for application: {} (error code: {}, error type: {})",
          _applicationName, code, type, e);
    }
  }

  private class RebuildRoutingTableMessageHandler extends MessageHandler {
    final String _tableNameWithType;

    RebuildRoutingTableMessageHandler(RoutingTableRebuildMessage routingTableRebuildMessage,
        NotificationContext context) {
      super(routingTableRebuildMessage, context);
      _tableNameWithType = routingTableRebuildMessage.getTableNameWithType();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _routingManager.buildRouting(_tableNameWithType);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while rebuilding routing table for table: {} (error code: {}, error type: {})",
          _tableNameWithType, code, type, e);
    }
  }

  private static class NoOpMessageHandler extends MessageHandler {

    NoOpMessageHandler(Message message, NotificationContext context) {
      super(message, context);
    }

    @Override
    public HelixTaskResult handleMessage() {
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error for no-op message handling (error code: {}, error type: {})", code, type, e);
    }
  }

  private static class QueryWorkloadRefreshMessageHandler extends MessageHandler {
    final String _queryWorkloadName;
    final InstanceCost _instanceCost;

    QueryWorkloadRefreshMessageHandler(QueryWorkloadRefreshMessage queryWorkloadRefreshMessage,
        NotificationContext context) {
      super(queryWorkloadRefreshMessage, context);
      _queryWorkloadName = queryWorkloadRefreshMessage.getQueryWorkloadName();
      _instanceCost = queryWorkloadRefreshMessage.getInstanceCost();
    }

    @Override
    public HelixTaskResult handleMessage() {
      // TODO: Add logic to invoke the query workload manager to refresh/delete the query workload config
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      LOGGER.error("Got error while refreshing query workload config for query workload: {} (error code: {},"
          + " error type: {})", _queryWorkloadName, errorCode, errorType, e);
    }
  }
}
