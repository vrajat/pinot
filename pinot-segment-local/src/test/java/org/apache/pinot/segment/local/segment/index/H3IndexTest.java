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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.realtime.impl.geospatial.MutableH3Index;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OnHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.index.h3.H3IndexType;
import org.apache.pinot.segment.local.segment.index.readers.geospatial.ImmutableH3IndexReader;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.apache.pinot.segment.local.utils.H3Utils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.config.table.FieldConfig.EncodingType.RAW;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class H3IndexTest implements PinotBuffersAfterMethodCheckRule {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "H3IndexCreatorTest");
  private static final Random RANDOM = new Random();

  @BeforeClass
  public void setUp()
      throws Exception {
    if (TEMP_DIR.exists()) {
      FileUtils.forceDelete(TEMP_DIR);
    }
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testH3Index()
      throws Exception {
    int numUniqueH3Ids = 123_456;
    Map<Long, Integer> expectedCardinalities = new HashMap<>();
    String onHeapColumnName = "onHeap";
    String offHeapColumnName = "offHeap";
    int resolution = 5;
    H3IndexResolution h3IndexResolution = new H3IndexResolution(Collections.singletonList(resolution));

    try (MutableH3Index mutableH3Index = new MutableH3Index(h3IndexResolution)) {
      try (GeoSpatialIndexCreator onHeapCreator = new OnHeapH3IndexCreator(TEMP_DIR, onHeapColumnName,
          "myTable_OFFLINE", false, h3IndexResolution);
          GeoSpatialIndexCreator offHeapCreator = new OffHeapH3IndexCreator(TEMP_DIR, offHeapColumnName,
              "myTable_OFFLINE", false, h3IndexResolution)) {
        int docId = 0;
        while (expectedCardinalities.size() < numUniqueH3Ids) {
          double longitude = RANDOM.nextDouble() * 360 - 180;
          double latitude = RANDOM.nextDouble() * 180 - 90;
          Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude));
          onHeapCreator.add(point);
          offHeapCreator.add(point);
          mutableH3Index.add(GeometrySerializer.serialize(point), -1, docId++);
          long h3Id = H3Utils.H3_CORE.latLngToCell(latitude, longitude, resolution);
          expectedCardinalities.merge(h3Id, 1, Integer::sum);
        }
        onHeapCreator.seal();
        offHeapCreator.seal();
      }

      File onHeapH3IndexFile = new File(TEMP_DIR, onHeapColumnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
      File offHeapH3IndexFile = new File(TEMP_DIR, offHeapColumnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
      try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapH3IndexFile);
          PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapH3IndexFile);
          H3IndexReader onHeapIndexReader = new ImmutableH3IndexReader(onHeapDataBuffer);
          H3IndexReader offHeapIndexReader = new ImmutableH3IndexReader(offHeapDataBuffer)) {
        H3IndexReader[] indexReaders = new H3IndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableH3Index};
        for (H3IndexReader indexReader : indexReaders) {
          Assert.assertEquals(indexReader.getH3IndexResolution().getLowestResolution(), resolution);
          for (Map.Entry<Long, Integer> entry : expectedCardinalities.entrySet()) {
            Assert.assertEquals(indexReader.getDocIds(entry.getKey()).getCardinality(), (int) entry.getValue());
          }
        }
      }
    }
  }

  @Test
  public void testSkipInvalidGeometry()
      throws Exception {
    String columnName = "skipInvalid";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", true,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20));
      creator.add(point);

      // Invalid serialized bytes should be skipped without throwing exception
      creator.add(new byte[]{1, 2, 3}, -1);

      creator.seal();
    }

    File indexFile = new File(TEMP_DIR, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        H3IndexReader reader = new ImmutableH3IndexReader(buffer)) {
      long h3Id = H3Utils.H3_CORE.latLngToCell(20, 10, res);
      Assert.assertEquals(reader.getDocIds(h3Id).getCardinality(), 1);
    }
  }

  @Test
  public void testSkipNullGeometry()
      throws Exception {
    String columnName = "skipNull";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", true,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20));
      creator.add(point);

      // Explicit null geometry should also be skipped
      creator.add(null);

      creator.seal();
    }

    File indexFile = new File(TEMP_DIR, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        H3IndexReader reader = new ImmutableH3IndexReader(buffer)) {
      long h3Id = H3Utils.H3_CORE.latLngToCell(20, 10, res);
      Assert.assertEquals(reader.getDocIds(h3Id).getCardinality(), 1);
    }
  }

  @Test
  public void testSkipNonPointGeometry()
      throws Exception {
    String columnName = "skipInvalidGeometryType";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", true,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 42));
      creator.add(point);

      // Explicit non-point geometry should also be skipped
      Point[] points = new Point[1];
      points[0] = point;
      MultiPoint multiPoint = GeometryUtils.GEOMETRY_FACTORY.createMultiPoint(points);
      creator.add(multiPoint);

      creator.seal();
    }

    File indexFile = new File(TEMP_DIR, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        H3IndexReader reader = new ImmutableH3IndexReader(buffer)) {
      long h3Id = H3Utils.H3_CORE.latLngToCell(42, 10, res);
      Assert.assertEquals(reader.getDocIds(h3Id).getCardinality(), 1);
    }
  }

  @Test
  public void testSkipInvalidGeometryContinueOnErrorFalse()
      throws Exception {
    String columnName = "skipInvalid";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", false,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20));
      creator.add(point);

      // Invalid serialized bytes should be skipped without throwing exception
      Assert.assertThrows(IllegalStateException.class, () -> creator.add(new byte[]{1, 2, 3}, -1));
    }
  }

  @Test
  public void testSkipNullGeometryContinueOnErrorFalse()
      throws Exception {
    String columnName = "skipNull";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", false,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20));
      creator.add(point);

      // Explicit null geometry should also be skipped
      Assert.assertThrows(IllegalStateException.class, () -> creator.add(null));
    }
  }

  @Test
  public void testSkipNonPointGeometryContinueOnErrorFalse()
      throws Exception {
    String columnName = "skipInvalidGeometryType";
    int res = 5;
    H3IndexResolution resolution = new H3IndexResolution(Collections.singletonList(res));

    try (GeoSpatialIndexCreator creator = new OnHeapH3IndexCreator(TEMP_DIR, columnName, "myTable_OFFLINE", false,
        resolution)) {
      Point point = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 42));
      creator.add(point);

      // Explicit non-point geometry should also be skipped
      Point[] points = new Point[1];
      points[0] = point;
      MultiPoint multiPoint = GeometryUtils.GEOMETRY_FACTORY.createMultiPoint(points);
      Assert.assertThrows(IllegalStateException.class, () -> creator.add(multiPoint));
    }
  }

  public static class ConfTest extends AbstractSerdeIndexContract {

    protected void assertEquals(H3IndexConfig expected) {
      Assert.assertEquals(getActualConfig("dimStr", StandardIndexes.h3()), expected);
    }

    @Test
    public void oldFieldConfigNull()
        throws JsonProcessingException {
      _tableConfig.setFieldConfigList(null);

      assertEquals(H3IndexConfig.DISABLED);
    }

    @Test
    public void oldEmptyFieldConfig()
        throws JsonProcessingException {
      cleanFieldConfig();

      assertEquals(H3IndexConfig.DISABLED);
    }

    @Test
    public void oldFieldConfigNotH3()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : []\n"
          + " }");

      assertEquals(H3IndexConfig.DISABLED);
    }

    @Test
    public void oldFieldConfigH3Resolution()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexTypes\" : [\"H3\"],\n"
          + "    \"properties\": {\n"
          + "       \"resolutions\": \"3\""
          + "     }\n"
          + " }");

      assertEquals(new H3IndexConfig(new H3IndexResolution(Lists.newArrayList(3))));
    }

    @Test
    public void newConfEnabled()
        throws JsonProcessingException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"h3\": {\n"
          + "          \"enabled\": \"true\",\n"
          + "          \"resolution\": [3]\n"
          + "       }\n"
          + "    }\n"
          + " }");
      assertEquals(new H3IndexConfig(new H3IndexResolution(Lists.newArrayList(3))));
    }

    @Test
    public void oldToNewConfConversion()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "    \"name\": \"dimStr\",\n"
          + "    \"indexes\" : {\n"
          + "       \"h3\": {\n"
          + "          \"enabled\": \"true\",\n"
          + "          \"resolution\": [3]\n"
          + "       }\n"
          + "    }\n"
          + " }");
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimStr"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(H3IndexType.INDEX_DISPLAY_NAME));
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
    }

    @Test
    public void testConvertToUpdatedFormat()
        throws IOException {
      addFieldIndexConfig("{\n"
          + "  \"name\": \"location_st_point\",\n"
          + "  \"encodingType\": \"RAW\",\n"
          + "  \"indexTypes\": [\n"
          + "    \"H3\"\n"
          + "  ],\n"
          + "  \"properties\": {\n"
          + "    \"resolutions\": \"13,5,6\"\n"
          + "  }\n"
          + "}");
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("location_st_point"))
          .collect(Collectors.toList()).get(0);
      Assert.assertEquals(fieldConfig.getEncodingType(), RAW);
      assertTrue(fieldConfig.getIndexTypes().isEmpty());
      assertNull(fieldConfig.getProperties());
      JsonNode node = fieldConfig.getIndexes().get(H3IndexType.INDEX_DISPLAY_NAME);
      Assert.assertEquals(node.toString(), "{\"disabled\":false,\"resolution\":[5,6,13]}");
    }
  }
}
