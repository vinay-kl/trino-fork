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
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.TrinoException;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogSchema;
import io.trino.unity.UnityCatalogTable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;

/**
 * In-memory implementation of Unity Catalog for testing.
 * Stores schemas and tables in local maps, keyed by catalog.schema and catalog.schema.table.
 */
final class TestingUnityCatalogClient
        extends UnityCatalogClient
{
    private final Map<String, UnityCatalogSchema> schemas = new ConcurrentHashMap<>();
    private final Map<String, UnityCatalogTable> tables = new ConcurrentHashMap<>();
    private volatile TemporaryCredentials temporaryCredentials;
    private final AtomicInteger credentialVendingCallCount = new AtomicInteger();
    private volatile String lastReceivedToken;

    TestingUnityCatalogClient()
    {
        super(new TestingHttpClient(request -> {
            throw new UnsupportedOperationException("TestingUnityCatalogClient should not make HTTP calls");
        }), URI.create("http://localhost:0"));
    }

    void addSchema(UnityCatalogSchema schema)
    {
        String key = schema.catalogName() + "." + schema.name();
        schemas.put(key, schema);
    }

    void addTable(UnityCatalogTable table)
    {
        String key = table.catalogName() + "." + table.schemaName() + "." + table.name();
        tables.put(key, table);
    }

    @Override
    public List<UnityCatalogSchema> listSchemas(String token, String catalogName)
    {
        this.lastReceivedToken = token;
        ImmutableList.Builder<UnityCatalogSchema> result = ImmutableList.builder();
        for (Map.Entry<String, UnityCatalogSchema> entry : schemas.entrySet()) {
            if (entry.getValue().catalogName().equals(catalogName)) {
                result.add(entry.getValue());
            }
        }
        return result.build();
    }

    @Override
    public Optional<UnityCatalogSchema> fetchSchema(String token, String catalogName, String schemaName)
    {
        this.lastReceivedToken = token;
        String key = catalogName + "." + schemaName;
        return Optional.ofNullable(schemas.get(key));
    }

    @Override
    public UnityCatalogSchema createSchema(String token, String catalogName, String schemaName, Optional<String> comment)
    {
        this.lastReceivedToken = token;
        UnityCatalogSchema schema = new UnityCatalogSchema(
                schemaName,
                catalogName,
                catalogName + "." + schemaName,
                "schema-id-" + schemaName,
                comment.orElse(null),
                null,
                null);
        addSchema(schema);
        return schema;
    }

    @Override
    public void deleteSchema(String token, String catalogName, String schemaName)
    {
        this.lastReceivedToken = token;
        String key = catalogName + "." + schemaName;
        if (schemas.remove(key) == null) {
            throw new TrinoException(NOT_FOUND, "Schema not found: " + key);
        }
        // Also remove all tables in this schema
        tables.entrySet().removeIf(entry -> entry.getKey().startsWith(key + "."));
    }

    @Override
    public List<UnityCatalogTable> listTables(String token, String catalogName, String schemaName)
    {
        this.lastReceivedToken = token;
        String prefix = catalogName + "." + schemaName + ".";
        ImmutableList.Builder<UnityCatalogTable> result = ImmutableList.builder();
        for (Map.Entry<String, UnityCatalogTable> entry : tables.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.add(entry.getValue());
            }
        }
        return result.build();
    }

    @Override
    public Optional<UnityCatalogTable> fetchTable(String token, String fullTableName)
    {
        this.lastReceivedToken = token;
        return Optional.ofNullable(tables.get(fullTableName));
    }

    @Override
    public UnityCatalogTable createTable(String token, Map<String, Object> tableRequest)
    {
        this.lastReceivedToken = token;
        String catalogName = (String) tableRequest.get("catalog_name");
        String schemaName = (String) tableRequest.get("schema_name");
        String tableName = (String) tableRequest.get("name");
        String storageLocation = (String) tableRequest.get("storage_location");
        String dataSourceFormat = (String) tableRequest.get("data_source_format");
        UnityCatalogTable table = new UnityCatalogTable(
                tableName,
                catalogName,
                schemaName,
                "EXTERNAL",
                dataSourceFormat,
                storageLocation,
                "table-id-" + tableName,
                null,
                null,
                null,
                null);
        addTable(table);
        return table;
    }

    @Override
    public void deleteTable(String token, String fullTableName)
    {
        this.lastReceivedToken = token;
        if (tables.remove(fullTableName) == null) {
            throw new TrinoException(NOT_FOUND, "Table not found: " + fullTableName);
        }
    }

    void setTemporaryCredentials(TemporaryCredentials temporaryCredentials)
    {
        this.temporaryCredentials = temporaryCredentials;
    }

    int credentialVendingCallCount()
    {
        return credentialVendingCallCount.get();
    }

    String lastReceivedToken()
    {
        return lastReceivedToken;
    }

    @Override
    public TemporaryCredentials generateTemporaryTableCredentials(String token, String tableId, String operation)
    {
        if (temporaryCredentials == null) {
            throw new UnsupportedOperationException("Credential vending not configured in testing client");
        }
        credentialVendingCallCount.incrementAndGet();
        return temporaryCredentials;
    }
}
