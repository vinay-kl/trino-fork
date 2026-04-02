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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.unity.StaticTokenProvider;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.TemporaryCredentials.AwsTempCredentials;
import io.trino.unity.UnityCatalogSchema;
import io.trino.unity.UnityCatalogTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.copyDirectoryContents;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestDeltaLakeUnityConnectorQueries
{
    private static final String UC_CATALOG_NAME = "unity_test";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String EMPTY_SCHEMA = "empty_schema";
    private static final String TEST_TABLE = "invariants";

    private DistributedQueryRunner queryRunner;
    private TestingUnityCatalogClient client;

    @BeforeAll
    void createQueryRunner()
            throws Exception
    {
        client = new TestingUnityCatalogClient();

        // Pre-populate schemas
        client.addSchema(new UnityCatalogSchema(
                TEST_SCHEMA,
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + "." + TEST_SCHEMA,
                "schema-id-1",
                null,
                null,
                null));
        client.addSchema(new UnityCatalogSchema(
                EMPTY_SCHEMA,
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + "." + EMPTY_SCHEMA,
                "schema-id-2",
                null,
                null,
                null));

        // Build query runner
        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(DELTA_CATALOG)
                                .setSchema(TEST_SCHEMA)
                                .build())
                .build();

        // Copy Delta table data
        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        Path tableDir = dataDir.resolve(TEST_SCHEMA).resolve(TEST_TABLE);
        Files.createDirectories(tableDir);
        Path sourceDir = new File(Resources.getResource("deltalake/invariants").toURI()).toPath();
        copyDirectoryContents(sourceDir, tableDir);

        // Register table in UC client
        String storageLocation = "local:///" + TEST_SCHEMA + "/" + TEST_TABLE;
        client.addTable(new UnityCatalogTable(
                TEST_TABLE,
                UC_CATALOG_NAME,
                TEST_SCHEMA,
                "EXTERNAL",
                "DELTA",
                storageLocation,
                "table-id-invariants",
                null,
                null,
                null,
                null));

        // Configure credential vending for write operation tests
        String expirationMs = String.valueOf(Instant.now().plus(Duration.ofHours(1)).toEpochMilli());
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("test-access-key", "test-secret-key", "test-session-token", expirationMs),
                null,
                null,
                null));

        // Use STATIC auth for simplicity
        StaticTokenProvider tokenProvider = new StaticTokenProvider("test-static-token");

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new TestingDeltaLakePlugin(
                dataDir,
                Optional.of(new TestingDeltaLakeUnityModule(client, tokenProvider, true))));

        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                .put("unity-catalog.server-uri", "http://unity-catalog.test.invalid:443")
                .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                .put("unity-catalog.auth-type", "STATIC")
                .put("unity-catalog.static-token", "test-static-token")
                .put("unity-catalog.allow-http-endpoint", "true")
                .put("unity-catalog.credential-vending-enabled", "true")
                .put("delta.enable-non-concurrent-writes", "true")
                .buildOrThrow());
    }

    @AfterAll
    void destroyQueryRunner()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    void testShowSchemas()
    {
        MaterializedResult result = queryRunner.execute("SHOW SCHEMAS FROM delta");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(TEST_SCHEMA, EMPTY_SCHEMA);
    }

    @Test
    void testShowTablesInSchema()
    {
        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM " + TEST_SCHEMA);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(TEST_TABLE);
    }

    @Test
    void testShowTablesInEmptySchema()
    {
        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM " + EMPTY_SCHEMA);
        assertThat(result.getMaterializedRows()).isEmpty();
    }

    @Test
    void testDescribeTable()
    {
        MaterializedResult result = queryRunner.execute("DESCRIBE " + TEST_SCHEMA + "." + TEST_TABLE);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("dummy");
    }

    @Test
    void testSelectStar()
    {
        MaterializedResult result = queryRunner.execute("SELECT * FROM " + TEST_SCHEMA + "." + TEST_TABLE);
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
    }

    @Test
    void testSelectWithPredicate()
    {
        MaterializedResult result = queryRunner.execute(
                "SELECT dummy FROM " + TEST_SCHEMA + "." + TEST_TABLE + " WHERE dummy = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
    }

    @Test
    void testSelectCount()
    {
        MaterializedResult result = queryRunner.execute(
                "SELECT count(*) FROM " + TEST_SCHEMA + "." + TEST_TABLE);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1L);
    }

    @Test
    void testInformationSchemaTables()
    {
        MaterializedResult result = queryRunner.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + TEST_SCHEMA + "'");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(TEST_TABLE);
    }

    @Test
    void testInformationSchemaColumns()
    {
        MaterializedResult result = queryRunner.execute(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + TEST_TABLE + "'");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("dummy");
    }

    @Test
    void testNonExistentSchemaError()
    {
        assertThatThrownBy(() -> queryRunner.execute("SHOW TABLES FROM nonexistent_schema"))
                .hasMessageContaining("nonexistent_schema");
    }

    @Test
    void testNonExistentTableError()
    {
        assertThatThrownBy(() -> queryRunner.execute(
                "SELECT * FROM " + TEST_SCHEMA + ".nonexistent_table"))
                .hasMessageContaining("nonexistent_table");
    }

    @Test
    void testRenameTableFails()
    {
        assertThatThrownBy(() -> queryRunner.execute(
                "ALTER TABLE " + TEST_SCHEMA + "." + TEST_TABLE + " RENAME TO " + TEST_SCHEMA + ".renamed_table"))
                .hasMessageContaining("Unity Catalog does not support renaming Delta Lake tables");
    }

    @Test
    void testCreateAndDropSchema()
    {
        String schemaName = "ddl_test_schema_" + randomNameSuffix();
        queryRunner.execute("CREATE SCHEMA delta." + schemaName);
        assertThat(queryRunner.execute("SHOW SCHEMAS FROM delta")
                .getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(schemaName);

        queryRunner.execute("DROP SCHEMA delta." + schemaName);
        assertThat(queryRunner.execute("SHOW SCHEMAS FROM delta")
                .getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .doesNotContain(schemaName);
    }

    @Test
    void testDropTable()
    {
        String tableName = "table_to_drop_" + randomNameSuffix();
        String storageLocation = "local:///" + TEST_SCHEMA + "/" + tableName;
        client.addTable(new UnityCatalogTable(
                tableName,
                UC_CATALOG_NAME,
                TEST_SCHEMA,
                "EXTERNAL",
                "DELTA",
                storageLocation,
                "table-id-" + tableName,
                null,
                null,
                null,
                null));

        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
        assertThat(queryRunner.execute("SHOW TABLES FROM " + TEST_SCHEMA)
                .getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .doesNotContain(tableName);
    }

    @Test
    void testNonDeltaTableFilteredFromListing()
    {
        // Register a non-Delta table (Parquet format) — should be silently filtered out
        String tableName = "parquet_table_" + randomNameSuffix();
        client.addTable(new UnityCatalogTable(
                tableName,
                UC_CATALOG_NAME,
                TEST_SCHEMA,
                "EXTERNAL",
                "PARQUET",
                "local:///" + TEST_SCHEMA + "/" + tableName,
                "table-id-" + tableName,
                null,
                null,
                null,
                null));

        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM " + TEST_SCHEMA);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .doesNotContain(tableName);
    }

    @Test
    void testCreateTableAsSelect()
    {
        String tableName = "ctas_table_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1, 'hello' AS col2");

        MaterializedResult result = queryRunner.execute("SELECT * FROM " + TEST_SCHEMA + "." + tableName);
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("hello");

        // Verify table was registered in UC
        assertThat(queryRunner.execute("SHOW TABLES FROM " + TEST_SCHEMA)
                .getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(tableName);

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testCreateTableAsSelectRequestsPathCredentials()
    {
        String tableName = "ctas_path_cred_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;

        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        // CTAS should request path credentials (not table credentials) since the table doesn't exist yet
        List<TestingUnityCatalogClient.PathCredentialRequest> tableRequests = client.pathCredentialRequests().stream()
                .filter(req -> req.url().contains(tableName))
                .toList();
        assertThat(tableRequests).isNotEmpty();
        assertThat(tableRequests).allMatch(req -> "PATH_CREATE_TABLE".equals(req.operation()));

        // Verify the table was created and data is readable
        MaterializedResult result = queryRunner.execute("SELECT * FROM " + TEST_SCHEMA + "." + tableName);
        assertThat(result.getRowCount()).isEqualTo(1);

        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testCreateTableDdlRequestsPathCredentials()
    {
        String tableName = "create_ddl_path_cred_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;

        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " (col1 INTEGER) WITH (location = '" + location + "')");

        // DDL CREATE TABLE should also request path credentials
        List<TestingUnityCatalogClient.PathCredentialRequest> tableRequests = client.pathCredentialRequests().stream()
                .filter(req -> req.url().contains(tableName))
                .toList();
        assertThat(tableRequests).isNotEmpty();
        assertThat(tableRequests).allMatch(req -> "PATH_CREATE_TABLE".equals(req.operation()));

        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testInsertRequestsWriteCredentials()
    {
        String tableName = "insert_cred_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        // The table now exists in UC with tableId "table-id-<name>"
        String tableId = "table-id-" + tableName;

        queryRunner.execute("INSERT INTO " + TEST_SCHEMA + "." + tableName + " VALUES (2)");

        // Verify credential vending was called with READ_WRITE for this table
        assertThat(client.operationsForTable(tableId)).contains("READ_WRITE");

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testAlterTableAddColumnRequestsWriteCredentials()
    {
        String tableName = "alter_col_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        String tableId = "table-id-" + tableName;

        queryRunner.execute("ALTER TABLE " + TEST_SCHEMA + "." + tableName + " ADD COLUMN col2 VARCHAR");

        // DDL operations write to _delta_log/ and require READ_WRITE credentials
        assertThat(client.operationsForTable(tableId)).contains("READ_WRITE");

        // Verify column was added
        MaterializedResult columns = queryRunner.execute("DESCRIBE " + TEST_SCHEMA + "." + tableName);
        assertThat(columns.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains("col1", "col2");

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testCommentOnTableRequestsWriteCredentials()
    {
        String tableName = "comment_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        String tableId = "table-id-" + tableName;

        queryRunner.execute("COMMENT ON TABLE " + TEST_SCHEMA + "." + tableName + " IS 'test comment'");

        // COMMENT writes to _delta_log/ and requires READ_WRITE credentials
        assertThat(client.operationsForTable(tableId)).contains("READ_WRITE");

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testDeleteRequestsWriteCredentials()
    {
        String tableName = "delete_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        String tableId = "table-id-" + tableName;

        queryRunner.execute("DELETE FROM " + TEST_SCHEMA + "." + tableName + " WHERE col1 = 1");

        assertThat(client.operationsForTable(tableId)).contains("READ_WRITE");

        // Verify data was deleted
        MaterializedResult result = queryRunner.execute("SELECT count(*) FROM " + TEST_SCHEMA + "." + tableName);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(0L);

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testUpdateRequestsWriteCredentials()
    {
        String tableName = "update_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        String tableId = "table-id-" + tableName;

        queryRunner.execute("UPDATE " + TEST_SCHEMA + "." + tableName + " SET col1 = 42 WHERE col1 = 1");

        assertThat(client.operationsForTable(tableId)).contains("READ_WRITE");

        // Verify data was updated
        MaterializedResult result = queryRunner.execute("SELECT col1 FROM " + TEST_SCHEMA + "." + tableName);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(42);

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }

    @Test
    void testSelectRequestsReadCredentials()
    {
        // Use a dedicated table to avoid interference from concurrent tests
        String tableName = "select_cred_test_" + randomNameSuffix();
        String location = "local:///" + TEST_SCHEMA + "/" + tableName;
        queryRunner.execute("CREATE TABLE " + TEST_SCHEMA + "." + tableName
                + " WITH (location = '" + location + "')"
                + " AS SELECT 1 AS col1");

        String tableId = "table-id-" + tableName;

        // Clear any CTAS operations by noting current count
        int operationsBefore = client.operationsForTable(tableId).size();

        queryRunner.execute("SELECT * FROM " + TEST_SCHEMA + "." + tableName);

        // SELECT should only use READ credentials, never READ_WRITE
        List<String> readOperations = client.operationsForTable(tableId).subList(
                operationsBefore, client.operationsForTable(tableId).size());
        assertThat(readOperations).isNotEmpty();
        assertThat(readOperations).allMatch("READ"::equals);

        // Clean up
        queryRunner.execute("DROP TABLE " + TEST_SCHEMA + "." + tableName);
    }
}
