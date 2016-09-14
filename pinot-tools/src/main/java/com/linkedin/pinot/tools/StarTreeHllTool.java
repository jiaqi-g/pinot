package com.linkedin.pinot.tools;

import com.linkedin.pinot.core.startree.hll.HllConfig;
import com.linkedin.pinot.tools.admin.command.CreateSegmentCommand2;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Tool to create StarTree segment with HLL locally.
 *
 * Run with JVM args -Xmx8192m
 */
public class StarTreeHllTool {
  private static final String AVRO_DATA_DIR = "segment"; // extract all data under it
  private static final int log2m = 7;
  private static final Set<String> columnsToDeriveHllFields =
      new HashSet<>(Arrays.asList("member_sk"));

  /**
   * Create StarTree segments with HLL by take all avro files under AVRO_DATA_DIR
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String tableName = "testTable";
    String segmentName = "testSegment";
    String inputDataDir = StarTreeHllTool.class.getClassLoader().getResource(AVRO_DATA_DIR).getFile();
    String outputSegmentDir =
        Files.createTempDirectory(StarTreeHllTool.class.getName() + "_" + tableName).toFile().getAbsolutePath();

    HllConfig hllConfig = new HllConfig(log2m, columnsToDeriveHllFields, "_hll");

    CreateSegmentCommand2 segmentCreator =
        new CreateSegmentCommand2().setDataDir(inputDataDir)
            .setTableName(tableName).setSegmentName(segmentName)
            .setOutDir(outputSegmentDir)
            .setOverwrite(true)
            .setEnableStarTreeIndex(true)
            .setHllConfig(hllConfig);

    segmentCreator.execute();
  }
}
