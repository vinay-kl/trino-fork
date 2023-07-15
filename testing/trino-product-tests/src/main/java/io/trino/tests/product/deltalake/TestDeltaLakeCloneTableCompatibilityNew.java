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
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
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

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testVacuumOnShallowCloneTable()
    {
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-";
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            Set<String> baseTableActiveDataFiles = getActiveDataFiles(baseTable);
            log.info("baseTableActiveDataFiles {} ::: " + baseTableActiveDataFiles);
            Set<String> baseTableAllDataFiles = getAllDataFilesFromTableDirectory(directoryName + baseTable);
            log.info("baseTableAllDataFiles {} ::: " + baseTableAllDataFiles);
            assertThat(baseTableActiveDataFiles).hasSize(1).isEqualTo(baseTableAllDataFiles);
            onDelta().executeQuery("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");
            onDelta().executeQuery("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");
            Set<String> clonedTableV1ActiveDataFiles = getActiveDataFiles(clonedTable);
            log.info("clonedTableV1ActiveDataFiles {} ::: " + clonedTableV1ActiveDataFiles);
            // size is 2 because, distinct path returns files which is union of base table (as of cloned version) and newly added file in cloned table
            assertThat(clonedTableV1ActiveDataFiles).hasSize(2);
            Set<String> clonedTableV1AllDataFiles = getAllDataFilesFromTableDirectory(directoryName + clonedTable);
            log.info("clonedTableV1AllDataFiles {} ::: " + clonedTableV1AllDataFiles);
            // size is 1 because, data file within shallow cloned folder is only 1 post the above insert
            assertThat(clonedTableV1AllDataFiles).hasSize(1);
            onDelta().executeQuery("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            Set<String> clonedTableV2ActiveDataFiles = getActiveDataFiles(clonedTable);
            log.info("clonedTableV2ActiveDataFiles {} ::: " + clonedTableV2ActiveDataFiles);
            // size is 2 because, referenced file from base table and relative file post above insert are both re-written
            assertThat(clonedTableV2ActiveDataFiles).hasSize(2);
            Set<String> clonedTableV2AllDataFiles = getAllDataFilesFromTableDirectory(directoryName + clonedTable);
            log.info("clonedTableV2AllDataFiles {} ::: " + clonedTableV2AllDataFiles);
            assertThat(clonedTableV2AllDataFiles).hasSize(3);
            onDelta().executeQuery("SET spark.databricks.delta.retentionDurationCheck.enabled = false");
            Set<String> toBeVacuumedDataFilesFromDryRun = getToBeVacuumedDataFilesFromDryRun(clonedTable);
            log.info("toBeVacuumedDataFilesFromDryRun {} ::: " + toBeVacuumedDataFilesFromDryRun);
            // only the clonedTableV1AllDataFiles should be deleted, which is size 1 and should not contain any files/paths from base table
            assertThat(toBeVacuumedDataFilesFromDryRun).hasSize(1)
                    .hasSameElementsAs(clonedTableV1AllDataFiles)
                    .doesNotContainAnyElementsOf(baseTableAllDataFiles);
            onDelta().executeQuery("VACUUM default." + clonedTable + " RETAIN 0 HOURS");
            Set<String> clonedTableV4ActiveDataFiles = getActiveDataFiles(clonedTable);
            log.info("clonedTableV4ActiveDataFiles {} ::: " + clonedTableV4ActiveDataFiles);
            // size of active data files should remain same
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2).isEqualTo(clonedTableV2ActiveDataFiles);
            Set<String> clonedTableV4AllDataFiles = getAllDataFilesFromTableDirectory(directoryName + clonedTable);
            log.info("clonedTableV4AllDataFiles {} ::: " + clonedTableV4AllDataFiles);
            // size of all data files should be 2 post vacuum
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2)
                    .hasSameElementsAs(clonedTableV4AllDataFiles);

            ImmutableList<Row> expectedRowsClonedTable = ImmutableList.of(row(2, "a"), row(3, "b"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);

            Row expectedRow = row(1, "a");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);

            Set<String> baseTableActiveDataFilesPostVacuumOnShallowClonedTable = getActiveDataFiles(baseTable);
            log.info("baseTableActiveDataFilesPostVacuumOnShallowClonedTable {} ::: " + baseTableActiveDataFilesPostVacuumOnShallowClonedTable);
            Set<String> baseTableAllDataFilesPostVacuumOnShallowClonedTable = getAllDataFilesFromTableDirectory(directoryName + baseTable);
            log.info("baseTableAllDataFilesPostVacuumOnShallowClonedTable {} ::: " + baseTableAllDataFilesPostVacuumOnShallowClonedTable);
            // nothing should've changed wrt base table
            assertThat(baseTableActiveDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableAllDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableActiveDataFiles)
                    .hasSameElementsAs(baseTableAllDataFiles);
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTable);
        }
    }

    private Set<String> getSingleColumnRows(QueryResult queryResult)
    {
        checkState(queryResult.getColumnTypes().size() == 1, "result set must have exactly one column");
        return queryResult.rows().stream()
                .map(objects -> objects.get(0))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private Set<String> getActiveDataFiles(String tableName)
    {
        return getSingleColumnRows(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + tableName));
    }

    private Set<String> getToBeVacuumedDataFilesFromDryRun(String tableName)
    {
        return getSingleColumnRows(onDelta().executeQuery("VACUUM default." + tableName + " RETAIN 0 HOURS DRY RUN"));
    }

    private Set<String> getAllDataFilesFromTableDirectory(String directory)
    {
        return s3.listObjectsV2(bucketName, directory).getObjectSummaries().stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().contains("/" + transactionLogDirectory))
                .map(s3ObjectSummary -> format("s3://%s/%s", bucketName, s3ObjectSummary.getKey()))
                .collect(toImmutableSet());
    }
}
