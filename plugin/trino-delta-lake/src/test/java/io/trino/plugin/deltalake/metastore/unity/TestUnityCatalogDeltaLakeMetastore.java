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

import io.trino.metastore.Database;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.unity.UnityCatalogSchema;
import io.trino.unity.UnityCatalogTable;
import org.junit.jupiter.api.Test;

import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory.DeltaLakeMetastores;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.unity.UnityCatalogConfig;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogDeltaLakeMetastore
{
    private static final String CATALOG = "unity";
    private static final String TOKEN = "test-token";

    // --- getAllDatabases ---

    @Test
    void testGetAllDatabasesEmpty()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());
        assertThat(metastore.getAllDatabases()).isEmpty();
    }

    @Test
    void testGetAllDatabases()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(deltaSchema("schema_a"));
        client.addSchema(deltaSchema("schema_b"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        List<String> databases = metastore.getAllDatabases();
        assertThat(databases).containsExactly("schema_a", "schema_b");
    }

    // --- getDatabase ---

    @Test
    void testGetDatabaseFound()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(new UnityCatalogSchema(
                "my_schema",
                CATALOG,
                CATALOG + ".my_schema",
                "schema-id-1",
                "A test schema",
                null,
                "alice"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<Database> database = metastore.getDatabase("my_schema");
        assertThat(database).isPresent();
        assertThat(database.get().getDatabaseName()).isEqualTo("my_schema");
        assertThat(database.get().getComment()).hasValue("A test schema");
        assertThat(database.get().getOwnerName()).hasValue("alice");
    }

    @Test
    void testGetDatabaseNotFound()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());
        assertThat(metastore.getDatabase("nonexistent")).isEmpty();
    }

    @Test
    void testGetDatabaseWithNullableFields()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(new UnityCatalogSchema("minimal", CATALOG, null, null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<Database> database = metastore.getDatabase("minimal");
        assertThat(database).isPresent();
        assertThat(database.get().getDatabaseName()).isEqualTo("minimal");
        assertThat(database.get().getComment()).isEmpty();
    }

    // --- getAllTables ---

    @Test
    void testGetAllTablesFiltersDelta()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("test_schema", "delta_table", "s3://bucket/delta_table"));
        client.addTable(new UnityCatalogTable(
                "parquet_table", CATALOG, "test_schema", "EXTERNAL", "PARQUET",
                "s3://bucket/parquet_table", "id-2", null, null, null, null));
        client.addTable(new UnityCatalogTable(
                "csv_table", CATALOG, "test_schema", "EXTERNAL", "CSV",
                "s3://bucket/csv_table", "id-3", null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        List<TableInfo> tables = metastore.getAllTables("test_schema");
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0).tableName()).isEqualTo(new SchemaTableName("test_schema", "delta_table"));
    }

    @Test
    void testGetAllTablesEmpty()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());
        assertThat(metastore.getAllTables("empty_schema")).isEmpty();
    }

    @Test
    void testGetAllTablesPopulatesCache()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "cached_table", "s3://bucket/cached"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        // First call: listTables → populates cache
        metastore.getAllTables("schema");

        // Remove from client — subsequent getTable should use cache
        client.deleteTable(TOKEN, CATALOG + ".schema.cached_table");

        Optional<DeltaMetastoreTable> table = metastore.getTable("schema", "cached_table");
        assertThat(table).isPresent();
        assertThat(table.get().location()).isEqualTo("s3://bucket/cached");
    }

    // --- getTable ---

    @Test
    void testGetTable()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "table", "s3://bucket/path"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<DeltaMetastoreTable> table = metastore.getTable("schema", "table");
        assertThat(table).isPresent();
        assertThat(table.get().schemaTableName()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(table.get().location()).isEqualTo("s3://bucket/path");
        assertThat(table.get().managed()).isFalse();
        assertThat(table.get().catalogOwned()).isTrue();
        assertThat(table.get().tableId()).hasValue("table-id-table");
    }

    @Test
    void testGetTableWithNullTableId()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "no_id_table", CATALOG, "schema", "EXTERNAL", "DELTA",
                "s3://bucket/no-id", null, null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<DeltaMetastoreTable> table = metastore.getTable("schema", "no_id_table");
        assertThat(table).isPresent();
        assertThat(table.get().tableId()).isEmpty();
    }

    @Test
    void testGetTableNotFound()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());
        assertThat(metastore.getTable("schema", "nonexistent")).isEmpty();
    }

    @Test
    void testGetTableFiltersNonDelta()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "parquet_table", CATALOG, "schema", "EXTERNAL", "PARQUET",
                "s3://bucket/path", "id-1", null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        assertThat(metastore.getTable("schema", "parquet_table")).isEmpty();
    }

    // --- getRawMetastoreTable ---

    @Test
    void testGetRawMetastoreTableSyntheticConstruction()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "my_table", CATALOG, "my_schema", "EXTERNAL", "DELTA",
                "s3://my-bucket/my-table", "table-id-123",
                null, null, "A test table", "bob"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<Table> rawTable = metastore.getRawMetastoreTable("my_schema", "my_table");
        assertThat(rawTable).isPresent();

        Table table = rawTable.get();
        assertThat(table.getDatabaseName()).isEqualTo("my_schema");
        assertThat(table.getTableName()).isEqualTo("my_table");
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
        assertThat(table.getOwner()).hasValue("bob");
        assertThat(table.getParameters()).containsEntry(TABLE_PROVIDER_PROPERTY, TABLE_PROVIDER_VALUE);
        assertThat(table.getParameters()).containsEntry(Table.TABLE_COMMENT, "A test table");
        assertThat(table.getStorage().getSerdeParameters()).containsEntry(PATH_PROPERTY, "s3://my-bucket/my-table");
        assertThat(table.getDataColumns()).isEmpty();
        assertThat(table.getPartitionColumns()).isEmpty();
    }

    @Test
    void testGetRawMetastoreTableWithoutOptionalFields()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "bare_table", "s3://bucket/bare"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<Table> rawTable = metastore.getRawMetastoreTable("schema", "bare_table");
        assertThat(rawTable).isPresent();
        assertThat(rawTable.get().getParameters()).doesNotContainKey(Table.TABLE_COMMENT);
        assertThat(rawTable.get().getOwner()).isEmpty();
    }

    @Test
    void testGetRawMetastoreTableNotFound()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());
        assertThat(metastore.getRawMetastoreTable("schema", "missing")).isEmpty();
    }

    // --- createDatabase ---

    @Test
    void testCreateDatabase()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Database database = Database.builder()
                .setDatabaseName("new_schema")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .setComment(Optional.of("New schema"))
                .build();
        metastore.createDatabase(database);

        assertThat(metastore.getAllDatabases()).contains("new_schema");
        assertThat(metastore.getDatabase("new_schema")).isPresent();
    }

    @Test
    void testCreateDatabaseWithoutComment()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Database database = Database.builder()
                .setDatabaseName("no_comment_schema")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
        metastore.createDatabase(database);

        assertThat(metastore.getDatabase("no_comment_schema")).isPresent();
    }

    // --- dropDatabase ---

    @Test
    void testDropDatabase()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(deltaSchema("to_drop"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        assertThat(metastore.getDatabase("to_drop")).isPresent();
        metastore.dropDatabase("to_drop", false);
        assertThat(metastore.getDatabase("to_drop")).isEmpty();
    }

    // --- createTable ---

    @Test
    void testCreateTable()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Table hiveTable = UnityCatalogDeltaLakeMetastore.toSyntheticHiveTable(
                "schema", "new_table",
                deltaTable("schema", "new_table", "s3://bucket/new_table"));
        metastore.createTable(hiveTable, null);

        Optional<DeltaMetastoreTable> created = metastore.getTable("schema", "new_table");
        assertThat(created).isPresent();
        assertThat(created.get().location()).isEqualTo("s3://bucket/new_table");
    }

    // --- dropTable ---

    @Test
    void testDropTable()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "to_drop", "s3://bucket/to_drop"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        assertThat(metastore.getTable("schema", "to_drop")).isPresent();
        metastore.dropTable(new SchemaTableName("schema", "to_drop"), "s3://bucket/to_drop", false);
        assertThat(metastore.getTable("schema", "to_drop")).isEmpty();
    }

    // --- renameTable ---

    @Test
    void testRenameTableThrowsNotSupported()
    {
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(new TestingUnityCatalogClient());

        assertThatThrownBy(() -> metastore.renameTable(
                new SchemaTableName("schema", "old_name"),
                new SchemaTableName("schema", "new_name")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unity Catalog does not support renaming")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()));
    }

    // --- replaceTable ---

    @Test
    void testReplaceTableIsNoOp()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "existing", "s3://bucket/existing"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Table hiveTable = UnityCatalogDeltaLakeMetastore.toSyntheticHiveTable(
                "schema", "existing",
                deltaTable("schema", "existing", "s3://bucket/existing"));
        // replaceTable should not throw
        metastore.replaceTable(hiveTable, null);

        // Table should still be accessible
        assertThat(metastore.getTable("schema", "existing")).isPresent();
    }

    @Test
    void testGetTableWithNullStorageLocation()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "no_location", CATALOG, "schema", "EXTERNAL", "DELTA",
                null, "id-x", null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        // Tables without storage location should be filtered out
        assertThat(metastore.getTable("schema", "no_location")).isEmpty();
        assertThat(metastore.getRawMetastoreTable("schema", "no_location")).isEmpty();
    }

    @Test
    void testGetAllTablesFiltersNullStorageLocation()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "no_loc", CATALOG, "schema", "EXTERNAL", "DELTA",
                null, "id-1", null, null, null, null));
        client.addTable(deltaTable("schema", "has_loc", "s3://bucket/loc"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        List<TableInfo> tables = metastore.getAllTables("schema");
        assertThat(tables).hasSize(1);
        assertThat(tables.get(0).tableName().getTableName()).isEqualTo("has_loc");
    }

    @Test
    void testGetTableAcceptsMixedCaseDeltaFormat()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "lower_delta", CATALOG, "schema", "EXTERNAL", "delta",
                "s3://bucket/lower", "id-lower", null, null, null, null));
        client.addTable(new UnityCatalogTable(
                "mixed_delta", CATALOG, "schema", "EXTERNAL", "Delta",
                "s3://bucket/mixed", "id-mixed", null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        assertThat(metastore.getTable("schema", "lower_delta")).isPresent();
        assertThat(metastore.getTable("schema", "mixed_delta")).isPresent();
    }

    // --- VendedCredentialsHandle.of integration ---

    @Test
    void testVendedCredentialsHandlePropagatesTableId()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "vended_table", "s3://bucket/vended"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        DeltaMetastoreTable table = metastore.getTable("schema", "vended_table").orElseThrow();
        VendedCredentialsHandle handle = VendedCredentialsHandle.of(table);

        assertThat(handle.catalogOwned()).isTrue();
        assertThat(handle.managed()).isFalse();
        assertThat(handle.tableLocation()).isEqualTo("s3://bucket/vended");
        assertThat(handle.tableId()).hasValue("table-id-vended_table");
        assertThat(handle.vendedCredentials()).isEmpty();
    }

    @Test
    void testVendedCredentialsHandleEmptyTableId()
    {
        DeltaMetastoreTable table = new DeltaMetastoreTable(
                new SchemaTableName("schema", "hive_table"), true, "s3://bucket/hive", false, Optional.empty());
        VendedCredentialsHandle handle = VendedCredentialsHandle.of(table);

        assertThat(handle.catalogOwned()).isFalse();
        assertThat(handle.tableId()).isEmpty();
    }

    // --- toSyntheticHiveTable ---

    @Test
    void testToSyntheticHiveTableVerifyDeltaLakeTableCompatible()
    {
        UnityCatalogTable ucTable = new UnityCatalogTable(
                "test", CATALOG, "schema", "EXTERNAL", "DELTA",
                "s3://bucket/test", "id", null, null, null, "owner");
        Table syntheticTable = UnityCatalogDeltaLakeMetastore.toSyntheticHiveTable("schema", "test", ucTable);

        // The synthetic table should pass verifyDeltaLakeTable
        // (has EXTERNAL_TABLE type + spark.sql.sources.provider=DELTA)
        assertThat(syntheticTable.getParameters().get(TABLE_PROVIDER_PROPERTY)).isEqualToIgnoringCase("DELTA");
        assertThat(syntheticTable.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
    }

    // --- UnityCatalogDeltaLakeMetastoreFactory ---

    @Test
    void testFactoryCreatesDistinctInstances()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogDeltaLakeMetastoreFactory factory = new UnityCatalogDeltaLakeMetastoreFactory(
                client,
                identity -> TOKEN,
                createConfig());

        DeltaLakeMetastores first = factory.createMetastores(ConnectorIdentity.ofUser("alice"));
        DeltaLakeMetastores second = factory.createMetastores(ConnectorIdentity.ofUser("alice"));
        assertThat(first.metastore()).isNotSameAs(second.metastore());
        assertThat(first.viewMetastore()).isEmpty();
        assertThat(second.viewMetastore()).isEmpty();
    }

    @Test
    void testFactoryRejectsNullClient()
    {
        assertThatThrownBy(() -> new UnityCatalogDeltaLakeMetastoreFactory(null, identity -> TOKEN, createConfig()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("client is null");
    }

    @Test
    void testFactoryRejectsNullTokenProvider()
    {
        assertThatThrownBy(() -> new UnityCatalogDeltaLakeMetastoreFactory(
                new TestingUnityCatalogClient(), null, createConfig()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tokenProvider is null");
    }

    @Test
    void testFactoryRejectsNullConfig()
    {
        assertThatThrownBy(() -> new UnityCatalogDeltaLakeMetastoreFactory(
                new TestingUnityCatalogClient(), identity -> TOKEN, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("config is null");
    }

    // --- managed table detection ---

    @Test
    void testGetTableManagedTableType()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(new UnityCatalogTable(
                "managed_table", CATALOG, "schema", "MANAGED", "DELTA",
                "s3://bucket/managed", "managed-id", null, null, null, null));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<DeltaMetastoreTable> table = metastore.getTable("schema", "managed_table");
        assertThat(table).isPresent();
        assertThat(table.get().managed()).isTrue();
        assertThat(table.get().catalogOwned()).isTrue();
    }

    @Test
    void testGetTableExternalTableType()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addTable(deltaTable("schema", "external_table", "s3://bucket/external"));
        UnityCatalogDeltaLakeMetastore metastore = createMetastore(client);

        Optional<DeltaMetastoreTable> table = metastore.getTable("schema", "external_table");
        assertThat(table).isPresent();
        assertThat(table.get().managed()).isFalse();
    }

    // --- Helpers ---

    private static UnityCatalogDeltaLakeMetastore createMetastore(TestingUnityCatalogClient client)
    {
        return new UnityCatalogDeltaLakeMetastore(client, TOKEN, CATALOG);
    }

    private static UnityCatalogSchema deltaSchema(String name)
    {
        return new UnityCatalogSchema(name, CATALOG, CATALOG + "." + name, "schema-id-" + name, null, null, null);
    }

    private static UnityCatalogTable deltaTable(String schema, String name, String location)
    {
        return new UnityCatalogTable(name, CATALOG, schema, "EXTERNAL", "DELTA", location, "table-id-" + name, null, null, null, null);
    }

    private static UnityCatalogConfig createConfig()
    {
        return new UnityCatalogConfig()
                .setServerUri(URI.create("http://localhost:0"))
                .setCatalogName(CATALOG)
                .setStaticToken(TOKEN)
                .setAllowHttpEndpoint(true);
    }
}
