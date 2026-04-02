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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Database;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.metastore.TableInfo.ExtendedRelationType;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogSchema;
import io.trino.unity.UnityCatalogTable;

import io.trino.spi.security.PrincipalType;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UnityCatalogDeltaLakeMetastore
        implements DeltaLakeMetastore
{
    private static final String DELTA_DATA_SOURCE_FORMAT = "DELTA";

    private final UnityCatalogClient client;
    private final String token;
    private final String catalogName;

    // Per-transaction cache: listTables populates this, getTable/getRawMetastoreTable use it
    private final ConcurrentHashMap<SchemaTableName, UnityCatalogTable> tableCache = new ConcurrentHashMap<>();

    public UnityCatalogDeltaLakeMetastore(UnityCatalogClient client, String token, String catalogName)
    {
        this.client = requireNonNull(client, "client is null");
        this.token = requireNonNull(token, "token is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return client.listSchemas(token, catalogName).stream()
                .map(UnityCatalogSchema::name)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return client.fetchSchema(token, catalogName, databaseName)
                .map(UnityCatalogDeltaLakeMetastore::toDatabase);
    }

    @Override
    public List<TableInfo> getAllTables(String databaseName)
    {
        List<UnityCatalogTable> tables = client.listTables(token, catalogName, databaseName);
        ImmutableList.Builder<TableInfo> result = ImmutableList.builder();
        for (UnityCatalogTable table : tables) {
            if (isDeltaTable(table)) {
                SchemaTableName schemaTableName = new SchemaTableName(databaseName, table.name());
                tableCache.put(schemaTableName, table);
                result.add(new TableInfo(schemaTableName, ExtendedRelationType.TABLE));
            }
        }
        return result.build();
    }

    @Override
    public Optional<Table> getRawMetastoreTable(String databaseName, String tableName)
    {
        return fetchUnityCatalogTable(databaseName, tableName)
                .map(ucTable -> toSyntheticHiveTable(databaseName, tableName, ucTable));
    }

    @Override
    public Optional<DeltaMetastoreTable> getTable(String databaseName, String tableName)
    {
        return fetchUnityCatalogTable(databaseName, tableName)
                .map(ucTable -> new DeltaMetastoreTable(
                        new SchemaTableName(databaseName, tableName),
                        isManagedTable(ucTable),
                        ucTable.storageLocation(),
                        true,
                        Optional.ofNullable(ucTable.tableId())));
    }

    @Override
    public void createDatabase(Database database)
    {
        client.createSchema(
                token,
                catalogName,
                database.getDatabaseName(),
                database.getComment());
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        client.deleteSchema(token, catalogName, databaseName);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        String storageLocation = table.getStorage().getSerdeParameters().get(PATH_PROPERTY);
        if (storageLocation == null) {
            storageLocation = table.getStorage().getOptionalLocation().orElse(null);
        }
        ImmutableMap.Builder<String, Object> request = ImmutableMap.<String, Object>builder()
                .put("catalog_name", catalogName)
                .put("schema_name", table.getDatabaseName())
                .put("name", table.getTableName())
                .put("table_type", table.getTableType().equals("MANAGED_TABLE") ? "MANAGED" : "EXTERNAL")
                .put("data_source_format", DELTA_DATA_SOURCE_FORMAT);
        if (storageLocation != null) {
            request.put("storage_location", storageLocation);
        }
        client.createTable(token, request.buildOrThrow());
    }

    @Override
    public void replaceTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        // Unity Catalog tables have immutable metadata; Delta Lake log is the source of truth.
        // This is called during table property updates (e.g., storing metadata in metastore).
        // For UC, the Delta log already has the changes, so this is a no-op.
    }

    @Override
    public void dropTable(SchemaTableName schemaTableName, String tableLocation, boolean deleteData)
    {
        String fullName = format("%s.%s.%s", catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        client.deleteTable(token, fullName);
        tableCache.remove(schemaTableName);
    }

    @Override
    public void renameTable(SchemaTableName from, SchemaTableName to)
    {
        throw new TrinoException(NOT_SUPPORTED, "Unity Catalog does not support renaming Delta Lake tables");
    }

    @Override
    public boolean isCredentialVendingEnabled()
    {
        return true;
    }

    private Optional<UnityCatalogTable> fetchUnityCatalogTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        UnityCatalogTable cached = tableCache.get(schemaTableName);
        if (cached != null) {
            return Optional.of(cached);
        }
        String fullName = format("%s.%s.%s", catalogName, databaseName, tableName);
        Optional<UnityCatalogTable> ucTable = client.fetchTable(token, fullName);
        ucTable.filter(UnityCatalogDeltaLakeMetastore::isDeltaTable)
                .ifPresent(table -> tableCache.put(schemaTableName, table));
        return ucTable.filter(UnityCatalogDeltaLakeMetastore::isDeltaTable);
    }

    private static boolean isDeltaTable(UnityCatalogTable table)
    {
        return DELTA_DATA_SOURCE_FORMAT.equalsIgnoreCase(table.dataSourceFormat())
                && table.storageLocation() != null;
    }

    private static boolean isManagedTable(UnityCatalogTable table)
    {
        return "MANAGED".equalsIgnoreCase(table.tableType());
    }

    private static Database toDatabase(UnityCatalogSchema schema)
    {
        Database.Builder builder = Database.builder()
                .setDatabaseName(schema.name())
                .setOwnerName(schema.optionalOwner())
                .setOwnerType(schema.optionalOwner().map(ignored -> PrincipalType.USER))
                .setComment(schema.optionalComment());
        return builder.build();
    }

    static Table toSyntheticHiveTable(String databaseName, String tableName, UnityCatalogTable ucTable)
    {
        requireNonNull(ucTable.storageLocation(), "storageLocation is null");
        Storage storage = Storage.builder()
                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT)
                .setLocation(Optional.ofNullable(ucTable.storageLocation()))
                .setSerdeParameters(ImmutableMap.of(PATH_PROPERTY, ucTable.storageLocation()))
                .build();
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.<String, String>builder()
                .put(TABLE_PROVIDER_PROPERTY, TABLE_PROVIDER_VALUE);
        ucTable.optionalComment().ifPresent(comment -> parameters.put(Table.TABLE_COMMENT, comment));
        return new Table(
                databaseName,
                tableName,
                ucTable.optionalOwner(),
                EXTERNAL_TABLE.name(),
                storage,
                ImmutableList.of(),
                ImmutableList.of(),
                parameters.buildOrThrow(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
    }
}
