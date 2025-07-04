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
package org.apache.pinot.common.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.metrics.NoopPinotMetricsRegistry;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_METRICS_NAME_PREFIX;


/**
 * Broker metrics utility class, which provides facilities to log the execution performance of queries.
 *
 */
public class BrokerMetrics extends AbstractMetrics<BrokerQueryPhase, BrokerMeter, BrokerGauge, BrokerTimer> {
  private static final BrokerMetrics NOOP = new BrokerMetrics(new NoopPinotMetricsRegistry());
  private static final AtomicReference<BrokerMetrics> BROKER_METRICS_INSTANCE = new AtomicReference<>(NOOP);
  private static final String PREFERRED_POOL_SET_TAG = "preferredPoolOptSet";
  private static final String PREFERRED_POOL_UNSET_TAG = "preferredPoolOptUnset";

  /**
   * register the brokerMetrics onto this class, so that we don't need to pass it down as a parameter
   */
  public static boolean register(BrokerMetrics brokerMetrics) {
    return BROKER_METRICS_INSTANCE.compareAndSet(NOOP, brokerMetrics);
  }

  /**
   * should always call after registration
   */
  public static BrokerMetrics get() {
    return BROKER_METRICS_INSTANCE.get();
  }

  /**
   * Constructs the broker metrics.
   *
   * @param metricsRegistry The metric registry used to register timers and meters.
   */
  public BrokerMetrics(PinotMetricsRegistry metricsRegistry) {
    this(metricsRegistry, DEFAULT_ENABLE_TABLE_LEVEL_METRICS, Collections.emptySet());
  }

  public BrokerMetrics(PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    this(DEFAULT_METRICS_NAME_PREFIX, metricsRegistry, isTableLevelMetricsEnabled, allowedTables);
  }

  public BrokerMetrics(String prefix, PinotMetricsRegistry metricsRegistry, boolean isTableLevelMetricsEnabled,
      Collection<String> allowedTables) {
    super(prefix, metricsRegistry, BrokerMetrics.class, isTableLevelMetricsEnabled, allowedTables);
  }

  @Override
  protected BrokerQueryPhase[] getQueryPhases() {
    return BrokerQueryPhase.values();
  }

  @Override
  protected BrokerMeter[] getMeters() {
    return BrokerMeter.values();
  }

  @Override
  protected BrokerGauge[] getGauges() {
    return BrokerGauge.values();
  }

  public static String getTagForPreferredPool(Map<String, String> queryOption) {
    if (queryOption == null) {
      return PREFERRED_POOL_UNSET_TAG;
    }
    // backward compatibility to check ORDERED_PREFERRED_REPLICAS here
    return (queryOption.containsKey(CommonConstants.Broker.Request.QueryOptionKey.ORDERED_PREFERRED_POOLS)
            || queryOption.containsKey(CommonConstants.Broker.Request.QueryOptionKey.ORDERED_PREFERRED_REPLICAS))
        ? PREFERRED_POOL_SET_TAG : PREFERRED_POOL_UNSET_TAG;
  }
}
