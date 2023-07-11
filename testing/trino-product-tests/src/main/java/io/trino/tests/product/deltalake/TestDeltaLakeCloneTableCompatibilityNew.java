/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.deltalake;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestDeltaLakeCloneTableCompatibilityNew
        extends BaseTestDeltaLakeS3Storage
{
    private static final Logger log = Logger.get(TestDeltaLakeCloneTableCompatibilityNew.class);
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;
    private final String transactionLogDirectory = "_delta_log";

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

//    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS},
//            dataProviderClass = DataProviders.class,
//            dataProvider = "trueFalse")
//    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
//    public void testReadFromSchemaChangedShallowCloneTable(boolean partitioned)
//    {
//        testReadSchemaChangedCloneTable("SHALLOW", partitioned);
//    }
//
//    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, PROFILE_SPECIFIC_TESTS},
//            dataProviderClass = DataProviders.class,
//            dataProvider = "trueFalse")
//    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
//    public void testReadFromSchemaChangedDeepCloneTable(boolean partitioned)
//    {
//        // Deep Clone is not supported on Delta-Lake OSS
//        testReadSchemaChangedCloneTable("DEEP", partitioned);
//    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testVacuumSupportForShallowCloneTable()
    {
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-" + baseTable;
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            QueryResult queryResult = onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + baseTable);
            log.info("rows::" + queryResult.rows());
            log.info("getActiveDataFiles:::" + getActiveDataFiles(baseTable));
            List<S3ObjectSummary> objectSummaries = s3.listObjectsV2(bucketName, directoryName).getObjectSummaries();
            log.info("objectSummaries:::" + objectSummaries);
            log.info("getAllDataFilesFromTableDirectory:::" + getAllDataFilesFromTableDirectory(directoryName));
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
        }
    }

    private Set<String> getActiveDataFiles(String tableName)
    {
        QueryResult queryResult = onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + tableName);
        checkState(queryResult.getColumnTypes().size() == 1, "result set must have exactly one column");
        return queryResult.rows().stream()
                .map(objects -> objects.get(0))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private Set<String> getAllDataFilesFromTableDirectory(String directory)
    {
        return s3.listObjectsV2(bucketName, directory).getObjectSummaries().stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().contains("/" + transactionLogDirectory))
                .map(s3ObjectSummary -> format("s3://%s/%s", bucketName, s3ObjectSummary.getKey()))
                .collect(toImmutableSet());
    }

//    private void testReadSchemaChangedCloneTable(String cloneType, boolean partitioned)
//    {
//        String baseTable = "test_dl_base_table_" + randomNameSuffix();
//        String clonedTableV1 = "test_dl_clone_tableV1_" + randomNameSuffix();
//        String clonedTableV2 = "test_dl_clone_tableV2_" + randomNameSuffix();
//        String clonedTableV3 = "test_dl_clone_tableV3_" + randomNameSuffix();
//        String clonedTableV4 = "test_dl_clone_tableV4_" + randomNameSuffix();
//        try {
//            onDelta().executeQuery("CREATE TABLE default." + baseTable +
//                    " (a_int INT, b_string STRING) USING delta " +
//                    (partitioned ? "PARTITIONED BY (b_string) " : "") +
//                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + baseTable + "'" +
//                    " TBLPROPERTIES (" +
//                    " 'delta.columnMapping.mode'='name' )");
//
//            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
//
//            Row expectedRow = row(1, "a");
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
//                    .containsOnly(expectedRow);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
//                    .containsOnly(expectedRow);
//
//            onDelta().executeQuery("ALTER TABLE default." + baseTable + " add columns (c_string string, d_int int)");
//
//            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (2, 'b', 'c', 3)");
//
//            onDelta().executeQuery("CREATE TABLE default." + clonedTableV1 +
//                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 1 " +
//                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + clonedTableV1 + "'");
//
//            Row expectedRowV1 = row(1, "a");
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 1"))
//                    .containsOnly(expectedRowV1);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV1))
//                    .containsOnly(expectedRowV1);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV1))
//                    .containsOnly(expectedRowV1);
//
//            onDelta().executeQuery("CREATE TABLE default." + clonedTableV2 +
//                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 2 " +
//                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + clonedTableV2 + "'");
//
//            Row expectedRowV2 = row(1, "a", null, null);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 2"))
//                    .containsOnly(expectedRowV2);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV2))
//                    .containsOnly(expectedRowV2);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV2))
//                    .containsOnly(expectedRowV2);
//
//            onDelta().executeQuery("CREATE TABLE default." + clonedTableV3 +
//                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 3 " +
//                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + clonedTableV3 + "'");
//
//            List<Row> expectedRowsV3 = ImmutableList.of(row(1, "a", null, null), row(2, "b", "c", 3));
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
//                    .containsOnly(expectedRowsV3);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
//                    .containsOnly(expectedRowsV3);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 3"))
//                    .containsOnly(expectedRowsV3);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV3))
//                    .containsOnly(expectedRowsV3);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV3))
//                    .containsOnly(expectedRowsV3);
//
//            onDelta().executeQuery("ALTER TABLE default." + baseTable + " DROP COLUMN c_string");
//            onDelta().executeQuery("CREATE TABLE default." + clonedTableV4 +
//                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 4 " +
//                    "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + clonedTableV4 + "'");
//
//            List<Row> expectedRowsV4 = ImmutableList.of(row(1, "a", null), row(2, "b", 3));
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
//                    .containsOnly(expectedRowsV4);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
//                    .containsOnly(expectedRowsV4);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 4"))
//                    .containsOnly(expectedRowsV4);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
//                    .containsOnly(expectedRowsV4);
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
//                    .containsOnly(expectedRowsV4);
//
//            if (partitioned) {
//                List<Row> expectedPartitionRows = ImmutableList.of(row("a"), row("b"));
//                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable))
//                        .containsOnly(expectedPartitionRows);
//                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + baseTable))
//                        .containsOnly(expectedPartitionRows);
//                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable + " VERSION AS OF 3"))
//                        .containsOnly(expectedPartitionRows);
//                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + clonedTableV3))
//                        .containsOnly(expectedPartitionRows);
//                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + clonedTableV3))
//                        .containsOnly(expectedPartitionRows);
//            }
//
//            onDelta().executeQuery("INSERT INTO default." + clonedTableV4 + " VALUES (3, 'c', 3)");
//            onTrino().executeQuery("INSERT INTO delta.default." + clonedTableV4 + " VALUES (4, 'd', 4)");
//
//            List<Row> expectedRowsV5 = ImmutableList.of(row(1, "a", null), row(2, "b", 3), row(3, "c", 3), row(4, "d", 4));
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
//                    .containsOnly(expectedRowsV5);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
//                    .containsOnly(expectedRowsV5);
//
//            onDelta().executeQuery("DELETE FROM default." + clonedTableV4 + " WHERE a_int in (1, 2)");
//
//            List<Row> expectedRowsV6 = ImmutableList.of(row(3, "c", 3), row(4, "d", 4));
//            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
//                    .containsOnly(expectedRowsV6);
//            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
//                    .containsOnly(expectedRowsV6);
//        }
//        finally {
//            dropDeltaTableWithRetry("default." + baseTable);
//            dropDeltaTableWithRetry("default." + clonedTableV1);
//            dropDeltaTableWithRetry("default." + clonedTableV2);
//            dropDeltaTableWithRetry("default." + clonedTableV3);
//            dropDeltaTableWithRetry("default." + clonedTableV4);
//        }
//    }
}
