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

        // Use STATIC auth for simplicity
        StaticTokenProvider tokenProvider = new StaticTokenProvider("test-static-token");

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new TestingDeltaLakePlugin(
                dataDir,
                () -> Optional.of(new TestingDeltaLakeUnityModule(client, tokenProvider))));

        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                .put("unity-catalog.server-uri", "http://localhost:0")
                .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                .put("unity-catalog.auth-type", "STATIC")
                .put("unity-catalog.static-token", "test-static-token")
                .put("unity-catalog.allow-http-endpoint", "true")
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
}
