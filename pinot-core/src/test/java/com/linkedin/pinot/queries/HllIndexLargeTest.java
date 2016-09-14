/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestSimpleAggreationQuery;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Split order: [profile_views, member_country_code, viewer_country_code, viewer_function_2_id, viewer_function_2_name, viewer_industry_group_name, viewer_seniority_2_name, viewer_seniority_2_id, member_geo_code, viewer_geo_code, is_internal_viewer, timestamp, unique_members_viewed, unique_profile_viewers]
 * Skip Materilazitaion For Dimensions: [viewer_sk, viewer_company_id, member_company_id, member_sk]
 */
public class HllIndexLargeTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllIndexLargeTest.class);

  private static final double approximationThreshold = 0.5;

  private static final String tableName = "testTable";
  private static final String segmentName = "testSegment_0";

  private static QueryExecutor QUERY_EXECUTOR;
  private static FileBasedInstanceDataManager instanceDataManager;
  private static PropertiesConfiguration serverConf;


  private static final Set<String> columnsToDeriveHllFields =
      new HashSet<>(Arrays.asList("member_sk"));

  private void setupTableManager() throws Exception {
    TableDataManagerProvider.setServerMetrics(new ServerMetrics(new MetricsRegistry()));
    serverConf =  new TestingServerPropertiesBuilder(tableName).build();
    serverConf.setDelimiterParsingDisabled(false);

    instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
  }

  @BeforeClass
  public void setup() throws Exception {
    setupTableManager();

    File segmentFile = new File("/Users/jgu/Desktop/hll_member_sk_6/testSegment_0");

    // Load Segment
    final IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.heap);
    System.out.println("Segment Name: " + indexSegment.getSegmentName());
    System.out.println("Loaded: " + Arrays.asList(indexSegment.getColumnNames()));
    instanceDataManager.getTableDataManager(tableName).addSegment(indexSegment);

    // Init Query Executor
    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, new ServerMetrics(
        new MetricsRegistry()));
  }

  @Test
  public void testFastHllNoGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"profile_views" /* first split */, "member_geo_code" /* low priority in split */};

    for (String filterColumn: filterColumns) {
      for (String distinctCountColumn : columnsToDeriveHllFields) {
        final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<>();
        aggCalls.add(new TestSimpleAggreationQuery(
            "select fasthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        aggCalls.add(new TestSimpleAggreationQuery(
            "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        ApproximateQueryTestUtil.runApproximationQueries(
            QUERY_EXECUTOR, segmentName, aggCalls, approximationThreshold);

        // correct query
        Object ret = ApproximateQueryTestUtil.runQuery(
            QUERY_EXECUTOR, segmentName, new TestSimpleAggreationQuery(
                "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                    " where " + filterColumn + " > " + baseValue + " limit 0",
                0.0));
        LOGGER.debug(ret.toString());
      }
    }
  }

  @Test
  public void testFastHllWithGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"profile_views" /* first split */, "member_geo_code" /* low priority in split */};
    final String[] gbyColumns = new String[]{"member_country_code", "viewer_industry_group_name", "viewer_geo_code", "profile_views"};

    for (String filterColumn: filterColumns) {
      for (String gbyColumn : gbyColumns) {
        for (String distinctCountColumn : columnsToDeriveHllFields) {
          final List<AvroQueryGenerator.TestGroupByAggreationQuery> groupByCalls = new ArrayList<>();
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select fasthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          ApproximateQueryTestUtil.runApproximationQueries(
              QUERY_EXECUTOR, segmentName, groupByCalls, approximationThreshold);

          // correct query
          Object ret = ApproximateQueryTestUtil.runQuery(
              QUERY_EXECUTOR, segmentName, new AvroQueryGenerator.TestGroupByAggreationQuery(
                  "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                      " where " + filterColumn + " < " + baseValue +
                      " group by " + gbyColumn + " limit 0", null));
          LOGGER.debug(ret.toString());
        }
      }
    }
  }
}
