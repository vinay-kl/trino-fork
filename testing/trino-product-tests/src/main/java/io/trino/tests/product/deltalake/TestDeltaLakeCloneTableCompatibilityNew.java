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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
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
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeCloneTableCompatibilityNew
        extends BaseTestDeltaLakeS3Storage
{
    private static final Logger log = Logger.get(TestDeltaLakeCloneTableCompatibilityNew.class);
    private final String transactionLogDirectory = "_delta_log";

    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTableChangesOnShallowCloneTable()
    {
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-tablechanges-compatibility-test-";
        String changeDataPrefix = directoryName + "/_change_data";
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'");
            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            onDelta().executeQuery("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'" +
                    " TBLPROPERTIES (delta.enableChangeDataFeed = true)");
            onDelta().executeQuery("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");
            Set<String> cdfFilesPostOnlyInsert = getFilesFromTableDirectory(changeDataPrefix);
            // Databricks version >= 12.2 keep an empty _change_data directory
            assertThat(cdfFilesPostOnlyInsert).hasSize(0);
            onDelta().executeQuery("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            Set<String> cdfFilesPostOnlyInsertAndUpdate = getFilesFromTableDirectory(changeDataPrefix);
            assertThat(cdfFilesPostOnlyInsertAndUpdate).hasSize(2);
            ImmutableList<Row> expectedRowsClonedTableOnTrino = ImmutableList.of(
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            // table_changes function from trino isn't considering `base table inserts on shallow cloned table`
            assertThat(onTrino().executeQuery("SELECT a, b, _change_type, _commit_version FROM TABLE(delta.table_changes('default', '" + clonedTable + "'"))
                    .containsOnly(expectedRowsClonedTableOnTrino);
            ImmutableList<Row> expectedRowsClonedTableOnSpark = ImmutableList.of(
                    row(1, "a", "insert", 0L),
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            assertThat(onDelta().executeQuery(
                    "SELECT a, b, _change_type, _commit_version FROM table_changes('default." + clonedTable + "', 0)"))
                    .containsOnly(expectedRowsClonedTableOnSpark);
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTable);
        }
    }

    private Set<String> getFilesFromTableDirectory(String directory)
    {
        return s3.listObjectsV2(bucketName, directory).getObjectSummaries().stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().contains("/" + transactionLogDirectory))
                .map(s3ObjectSummary -> format("s3://%s/%s", bucketName, s3ObjectSummary.getKey()))
                .collect(toImmutableSet());
    }
}
