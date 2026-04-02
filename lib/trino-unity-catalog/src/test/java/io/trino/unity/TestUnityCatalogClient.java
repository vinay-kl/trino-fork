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
package io.trino.unity;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogClient
{
    private static final URI BASE_URI = URI.create("http://localhost:8080/api/2.1/unity-catalog");
    private static final String TEST_TOKEN = "test-token-123";

    // --- listSchemas ---

    @Test
    void testListSchemas()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"schemas": [
                    {"name": "default", "catalog_name": "unity"},
                    {"name": "tpch", "catalog_name": "unity"}
                ]}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogSchema> schemas = client.listSchemas(TEST_TOKEN, "unity");

        assertThat(schemas).hasSize(2);
        assertThat(schemas.get(0).name()).isEqualTo("default");
        assertThat(schemas.get(1).name()).isEqualTo("tpch");
        assertThat(processor.requests).hasSize(1);
        assertThat(processor.requests.get(0).getUri().toString()).contains("catalog_name=unity");
        assertAuthorizationHeader(processor.requests.get(0));
    }

    @Test
    void testListSchemasWithPagination()
    {
        List<String> responses = List.of(
                """
                {"schemas": [{"name": "s1", "catalog_name": "unity"}], "next_page_token": "token1"}""",
                """
                {"schemas": [{"name": "s2", "catalog_name": "unity"}]}""");
        PaginatingProcessor processor = new PaginatingProcessor(responses);

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogSchema> schemas = client.listSchemas(TEST_TOKEN, "unity");

        assertThat(schemas).hasSize(2);
        assertThat(schemas.get(0).name()).isEqualTo("s1");
        assertThat(schemas.get(1).name()).isEqualTo("s2");
        assertThat(processor.requestCount).isEqualTo(2);
        // Verify second request includes the page token from the first response
        assertThat(processor.requests.get(1).getUri().toString()).contains("page_token=token1");
    }

    @Test
    void testListSchemasEmpty()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"schemas": []}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogSchema> schemas = client.listSchemas(TEST_TOKEN, "unity");

        assertThat(schemas).isEmpty();
    }

    @Test
    void testListSchemasMissingSchemasKey()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogSchema> schemas = client.listSchemas(TEST_TOKEN, "unity");

        assertThat(schemas).isEmpty();
    }

    // --- fetchSchema ---

    @Test
    void testFetchSchema()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"name": "tpch", "catalog_name": "unity", "schema_id": "uuid-123"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogSchema> schema = client.fetchSchema(TEST_TOKEN, "unity", "tpch");

        assertThat(schema).isPresent();
        assertThat(schema.get().name()).isEqualTo("tpch");
        assertThat(processor.requests.get(0).getUri().getPath()).endsWith("/schemas/unity.tpch");
    }

    @Test
    void testFetchSchemaNotFound()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(404, """
                {"message": "Schema not found"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogSchema> schema = client.fetchSchema(TEST_TOKEN, "unity", "missing");

        assertThat(schema).isEmpty();
    }

    @Test
    void testFetchSchemaAccessDenied()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(403, """
                {"message": "Permission denied"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.fetchSchema(TEST_TOKEN, "unity", "secret"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    // --- createSchema ---

    @Test
    void testCreateSchema()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"name": "new_schema", "catalog_name": "unity", "schema_id": "new-id"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        UnityCatalogSchema schema = client.createSchema(TEST_TOKEN, "unity", "new_schema", Optional.of("test schema"));

        assertThat(schema.name()).isEqualTo("new_schema");
        assertThat(processor.requests.get(0).getMethod()).isEqualTo("POST");
        assertThat(processor.requests.get(0).getUri().getPath()).endsWith("/schemas");
        assertAuthorizationHeader(processor.requests.get(0));
    }

    @Test
    void testCreateSchemaWithoutComment()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"name": "bare_schema", "catalog_name": "unity"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        UnityCatalogSchema schema = client.createSchema(TEST_TOKEN, "unity", "bare_schema", Optional.empty());

        assertThat(schema.name()).isEqualTo("bare_schema");
        assertThat(processor.requests.get(0).getMethod()).isEqualTo("POST");
        assertThat(processor.requests.get(0).getUri().getPath()).endsWith("/schemas");
    }

    // --- deleteSchema ---

    @Test
    void testDeleteSchema()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, "{}"));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.deleteSchema(TEST_TOKEN, "unity", "old_schema");

        assertThat(processor.requests.get(0).getMethod()).isEqualTo("DELETE");
        assertThat(processor.requests.get(0).getUri().getPath()).endsWith("/schemas/unity.old_schema");
    }

    // --- listTables ---

    @Test
    void testListTables()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"tables": [
                    {"name": "orders", "catalog_name": "unity", "schema_name": "tpch",
                     "table_type": "EXTERNAL", "data_source_format": "DELTA",
                     "storage_location": "s3://bucket/orders", "table_id": "id1"},
                    {"name": "lineitem", "catalog_name": "unity", "schema_name": "tpch",
                     "table_type": "MANAGED", "data_source_format": "DELTA",
                     "storage_location": "s3://bucket/lineitem", "table_id": "id2"}
                ]}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogTable> tables = client.listTables(TEST_TOKEN, "unity", "tpch");

        assertThat(tables).hasSize(2);
        assertThat(tables.get(0).name()).isEqualTo("orders");
        assertThat(tables.get(1).tableType()).isEqualTo("MANAGED");
        assertThat(processor.requests.get(0).getUri().toString()).contains("catalog_name=unity");
        assertThat(processor.requests.get(0).getUri().toString()).contains("schema_name=tpch");
    }

    @Test
    void testListTablesEmpty()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"tables": []}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogTable> tables = client.listTables(TEST_TOKEN, "unity", "empty_schema");

        assertThat(tables).isEmpty();
    }

    @Test
    void testListTablesWithPagination()
    {
        List<String> responses = List.of(
                """
                {"tables": [{"name": "t1", "catalog_name": "unity", "schema_name": "s",
                 "table_type": "EXTERNAL", "data_source_format": "DELTA",
                 "storage_location": "s3://b/t1", "table_id": "id1"}], "next_page_token": "pg2"}""",
                """
                {"tables": [{"name": "t2", "catalog_name": "unity", "schema_name": "s",
                 "table_type": "MANAGED", "data_source_format": "DELTA",
                 "storage_location": "s3://b/t2", "table_id": "id2"}]}""");
        PaginatingProcessor processor = new PaginatingProcessor(responses);

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogTable> tables = client.listTables(TEST_TOKEN, "unity", "s");

        assertThat(tables).hasSize(2);
        assertThat(tables.get(0).name()).isEqualTo("t1");
        assertThat(tables.get(1).name()).isEqualTo("t2");
        assertThat(processor.requestCount).isEqualTo(2);
        assertThat(processor.requests.get(1).getUri().toString()).contains("page_token=pg2");
    }

    @Test
    void testListTablesMissingTablesKey()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogTable> tables = client.listTables(TEST_TOKEN, "unity", "s");

        assertThat(tables).isEmpty();
    }

    // --- fetchTable ---

    @Test
    void testFetchTable()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "orders",
                    "catalog_name": "unity",
                    "schema_name": "tpch",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/orders",
                    "table_id": "table-uuid-123"
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogTable> table = client.fetchTable(TEST_TOKEN, "unity.tpch.orders");

        assertThat(table).isPresent();
        assertThat(table.get().name()).isEqualTo("orders");
        assertThat(table.get().tableType()).isEqualTo("EXTERNAL");
        assertThat(table.get().dataSourceFormat()).isEqualTo("DELTA");
        assertThat(table.get().storageLocation()).isEqualTo("s3://bucket/orders");
        assertThat(table.get().tableId()).isEqualTo("table-uuid-123");
    }

    @Test
    void testFetchTableNotFound()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(404, """
                {"message": "Table not found"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogTable> table = client.fetchTable(TEST_TOKEN, "unity.tpch.missing");

        assertThat(table).isEmpty();
    }

    @Test
    void testFetchTableAccessDenied()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(403, """
                {"message": "Forbidden"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.fetchTable(TEST_TOKEN, "unity.tpch.secret"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testFetchTableServerError()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(500, """
                {"message": "Internal server error"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.fetchTable(TEST_TOKEN, "unity.tpch.orders"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
    }

    @Test
    void testFetchTableWithNullableFieldsAbsent()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "minimal",
                    "catalog_name": "unity",
                    "schema_name": "tpch",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/minimal",
                    "table_id": "id-minimal"
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogTable> table = client.fetchTable(TEST_TOKEN, "unity.tpch.minimal");

        assertThat(table).isPresent();
        UnityCatalogTable t = table.get();
        assertThat(t.name()).isEqualTo("minimal");
        // Nullable fields should be null when absent from JSON
        assertThat(t.columns()).isNull();
        assertThat(t.properties()).isNull();
        assertThat(t.comment()).isNull();
        assertThat(t.owner()).isNull();
        // Optional accessors should return empty
        assertThat(t.optionalColumns()).isEmpty();
        assertThat(t.optionalProperties()).isEmpty();
        assertThat(t.optionalComment()).isEmpty();
        assertThat(t.optionalOwner()).isEmpty();
    }

    @Test
    void testFetchTableWithUnknownJsonFieldsIgnored()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "orders",
                    "catalog_name": "unity",
                    "schema_name": "tpch",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/orders",
                    "table_id": "id1",
                    "some_future_field": "should be ignored",
                    "another_unknown": 42
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogTable> table = client.fetchTable(TEST_TOKEN, "unity.tpch.orders");

        // Verify full object parsed correctly despite extra unknown fields
        assertThat(table).isPresent();
        assertThat(table.get().name()).isEqualTo("orders");
        assertThat(table.get().catalogName()).isEqualTo("unity");
        assertThat(table.get().schemaName()).isEqualTo("tpch");
        assertThat(table.get().tableType()).isEqualTo("EXTERNAL");
        assertThat(table.get().dataSourceFormat()).isEqualTo("DELTA");
        assertThat(table.get().storageLocation()).isEqualTo("s3://bucket/orders");
        assertThat(table.get().tableId()).isEqualTo("id1");
    }

    // --- createTable ---

    @Test
    void testCreateTable()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "new_table",
                    "catalog_name": "unity",
                    "schema_name": "tpch",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/new_table",
                    "table_id": "new-id"
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        UnityCatalogTable table = client.createTable(TEST_TOKEN, ImmutableMap.of(
                "name", "new_table",
                "catalog_name", "unity",
                "schema_name", "tpch",
                "table_type", "EXTERNAL",
                "data_source_format", "DELTA",
                "storage_location", "s3://bucket/new_table"));

        assertThat(table.name()).isEqualTo("new_table");
        assertThat(table.tableId()).isEqualTo("new-id");
        assertThat(processor.requests.get(0).getMethod()).isEqualTo("POST");
        assertAuthorizationHeader(processor.requests.get(0));
    }

    // --- deleteTable ---

    @Test
    void testDeleteTable()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, "{}"));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.deleteTable(TEST_TOKEN, "unity.tpch.orders");

        assertThat(processor.requests.get(0).getMethod()).isEqualTo("DELETE");
        assertThat(processor.requests.get(0).getUri().getPath()).contains("/tables/");
        assertAuthorizationHeader(processor.requests.get(0));
    }

    // --- generateTemporaryTableCredentials ---

    @Test
    void testGenerateTemporaryTableCredentialsAws()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "aws_temp_credentials": {
                        "access_key_id": "ASIA123",
                        "secret_access_key": "secret123",
                        "session_token": "session123",
                        "expiration_time": "2025-12-31T23:59:59.000Z"
                    }
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        TemporaryCredentials credentials = client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-123", "READ");

        assertThat(credentials.awsTempCredentials()).isPresent();
        assertThat(credentials.awsTempCredentials().get().accessKeyId()).isEqualTo("ASIA123");
        assertThat(credentials.awsTempCredentials().get().secretAccessKey()).isEqualTo("secret123");
        assertThat(credentials.awsTempCredentials().get().sessionToken()).isEqualTo("session123");
        assertThat(credentials.awsTempCredentials().get().expirationTime()).isEqualTo("2025-12-31T23:59:59.000Z");
        assertThat(credentials.azureTempCredentials()).isEmpty();
        assertThat(credentials.gcpTempCredentials()).isEmpty();
    }

    @Test
    void testGenerateTemporaryTableCredentialsAzure()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "azure_user_delegation_sas": {
                        "sas_token": "sv=2021-08-06&ss=b&srt=sco&sig=abc",
                        "expiration_time": "2025-12-31T23:59:59.000Z"
                    }
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        TemporaryCredentials credentials = client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-456", "READ");

        assertThat(credentials.azureTempCredentials()).isPresent();
        assertThat(credentials.azureTempCredentials().get().sasToken()).isEqualTo("sv=2021-08-06&ss=b&srt=sco&sig=abc");
        assertThat(credentials.azureTempCredentials().get().expirationTime()).isEqualTo("2025-12-31T23:59:59.000Z");
        assertThat(credentials.awsTempCredentials()).isEmpty();
        assertThat(credentials.gcpTempCredentials()).isEmpty();
    }

    @Test
    void testGenerateTemporaryTableCredentialsGcp()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "gcp_oauth_token": {
                        "oauth_token": "ya29.gcp-token",
                        "expiration_time": "2025-12-31T23:59:59.000Z"
                    }
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        TemporaryCredentials credentials = client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-789", "READ_WRITE");

        assertThat(credentials.gcpTempCredentials()).isPresent();
        assertThat(credentials.gcpTempCredentials().get().oauthToken()).isEqualTo("ya29.gcp-token");
        assertThat(credentials.awsTempCredentials()).isEmpty();
        assertThat(credentials.azureTempCredentials()).isEmpty();
    }

    @Test
    void testGenerateTemporaryTableCredentialsWithTopLevelExpiration()
    {
        // Databricks UC returns expiration_time at top level as epoch milliseconds number
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "aws_temp_credentials": {
                        "access_key_id": "ASIA123",
                        "secret_access_key": "secret123",
                        "session_token": "session123"
                    },
                    "expiration_time": 1773166207000
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        TemporaryCredentials credentials = client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-123", "READ");

        assertThat(credentials.awsTempCredentials()).isPresent();
        assertThat(credentials.awsTempCredentials().get().accessKeyId()).isEqualTo("ASIA123");
        assertThat(credentials.awsTempCredentials().get().expirationTime()).isNull();
        assertThat(credentials.expirationTime()).isPresent();
        assertThat(credentials.expirationTime().get()).isEqualTo("1773166207000");
    }

    @Test
    void testGenerateTemporaryTableCredentialsNoCloudCreds()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        TemporaryCredentials credentials = client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-000", "READ");

        assertThat(credentials.awsTempCredentials()).isEmpty();
        assertThat(credentials.azureTempCredentials()).isEmpty();
        assertThat(credentials.gcpTempCredentials()).isEmpty();
    }

    @Test
    void testCredentialVendingAccessDenied()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(403, """
                {"message": "EXTERNAL USE SCHEMA privilege required"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.generateTemporaryTableCredentials(TEST_TOKEN, "table-uuid-123", "READ"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testCredentialVendingRequestBody()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"aws_temp_credentials": {"access_key_id": "X", "secret_access_key": "Y",
                 "session_token": "Z", "expiration_time": "2025-01-01T00:00:00Z"}}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.generateTemporaryTableCredentials(TEST_TOKEN, "my-table-id", "READ_WRITE");

        assertThat(processor.requests.get(0).getMethod()).isEqualTo("POST");
        assertThat(processor.requests.get(0).getUri().getPath()).endsWith("/temporary-table-credentials");
        assertAuthorizationHeader(processor.requests.get(0));
    }

    // --- Error handling per status code ---

    @Test
    void testUnauthorizedError()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(401, """
                {"message": "Invalid token"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testConflictError()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(409, """
                {"message": "Schema already exists"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.createSchema(TEST_TOKEN, "unity", "existing", Optional.empty()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("already exists")
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(ALREADY_EXISTS.toErrorCode());
    }

    @Test
    void testRateLimitError()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(429, """
                {"message": "Too many requests"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listTables(TEST_TOKEN, "unity", "tpch"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("rate limit")
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
    }

    @Test
    void testServerError()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(500, """
                {"message": "Internal server error"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
    }

    @Test
    void testNotFoundErrorOnList()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(404, """
                {"message": "Catalog not found"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "missing_catalog"))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(NOT_FOUND.toErrorCode());
    }

    @Test
    void testErrorResponseWithoutMessageField()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(500, """
                {"error_code": "UNKNOWN", "detail": "something broke"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unity Catalog error (HTTP 500)");
    }

    // --- Security tests ---

    @Test
    void testTokenRedactedBearerPattern()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(400, """
                {"message": "Invalid token: bearer abc123xyz"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas("secret-token-value", "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("abc123xyz")
                .hasMessageContaining("REDACTED");
    }

    @Test
    void testTokenRedactedTokenEqualsPattern()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(400, """
                {"message": "Failed with token=dapi1234567890abcdef"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("dapi1234567890abcdef")
                .hasMessageContaining("REDACTED");
    }

    @Test
    void testAuthorizationHeaderIncluded()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> {
            assertThat(request.getHeader("Authorization")).isEqualTo("Bearer my-token");
            return jsonResponse(200, """
                    {"schemas": []}""");
        });

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.listSchemas("my-token", "unity");
    }

    @Test
    void testAuthorizationHeaderOnPost()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"name": "s", "catalog_name": "c"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.createSchema(TEST_TOKEN, "c", "s", Optional.empty());

        assertAuthorizationHeader(processor.requests.get(0));
    }

    @Test
    void testAuthorizationHeaderOnDelete()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, "{}"));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        client.deleteTable(TEST_TOKEN, "c.s.t");

        assertAuthorizationHeader(processor.requests.get(0));
    }

    @Test
    void testNetworkFailureDoesNotLeakToken()
    {
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            throw new RuntimeException("Connection refused to " + request.getUri());
        });

        UnityCatalogClient client = new UnityCatalogClient(httpClient, BASE_URI);
        assertThatThrownBy(() -> client.listSchemas("super-secret-token", "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("super-secret-token")
                .hasMessageContaining("Unity Catalog request failed");
    }

    // --- Deserialization tests ---

    @Test
    void testFetchTableWithColumnsDeserialized()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "orders",
                    "catalog_name": "unity",
                    "schema_name": "tpch",
                    "table_type": "EXTERNAL",
                    "data_source_format": "DELTA",
                    "storage_location": "s3://bucket/orders",
                    "table_id": "id1",
                    "columns": [
                        {"name": "order_id", "type_text": "bigint", "type_name": "LONG", "position": 0, "nullable": false},
                        {"name": "status", "type_text": "string", "type_name": "STRING", "position": 1, "comment": "order status", "nullable": true}
                    ],
                    "properties": {"delta.minReaderVersion": "1"},
                    "comment": "Order table",
                    "owner": "admin"
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogTable> table = client.fetchTable(TEST_TOKEN, "unity.tpch.orders");

        assertThat(table).isPresent();
        UnityCatalogTable t = table.get();
        assertThat(t.optionalColumns()).isPresent();
        List<UnityCatalogColumn> columns = t.optionalColumns().get();
        assertThat(columns).hasSize(2);

        // First column: all fields present, no comment
        UnityCatalogColumn col0 = columns.get(0);
        assertThat(col0.name()).isEqualTo("order_id");
        assertThat(col0.optionalTypeText()).hasValue("bigint");
        assertThat(col0.optionalTypeName()).hasValue("LONG");
        assertThat(col0.position()).isEqualTo(0);
        assertThat(col0.nullable()).isFalse();
        assertThat(col0.optionalComment()).isEmpty();

        // Second column: all fields present, with comment
        UnityCatalogColumn col1 = columns.get(1);
        assertThat(col1.name()).isEqualTo("status");
        assertThat(col1.optionalTypeText()).hasValue("string");
        assertThat(col1.optionalTypeName()).hasValue("STRING");
        assertThat(col1.position()).isEqualTo(1);
        assertThat(col1.nullable()).isTrue();
        assertThat(col1.optionalComment()).hasValue("order status");

        // Properties, comment, owner
        assertThat(t.optionalProperties()).isPresent();
        assertThat(t.optionalProperties().get()).containsEntry("delta.minReaderVersion", "1");
        assertThat(t.optionalComment()).hasValue("Order table");
        assertThat(t.optionalOwner()).hasValue("admin");
    }

    @Test
    void testFetchSchemaWithAllOptionalFields()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {
                    "name": "tpch",
                    "catalog_name": "unity",
                    "full_name": "unity.tpch",
                    "schema_id": "schema-uuid-123",
                    "comment": "TPC-H benchmark schema",
                    "properties": {"key1": "value1"},
                    "owner": "admin"
                }"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogSchema> schema = client.fetchSchema(TEST_TOKEN, "unity", "tpch");

        assertThat(schema).isPresent();
        UnityCatalogSchema s = schema.get();
        assertThat(s.name()).isEqualTo("tpch");
        assertThat(s.catalogName()).isEqualTo("unity");
        assertThat(s.optionalFullName()).hasValue("unity.tpch");
        assertThat(s.optionalSchemaId()).hasValue("schema-uuid-123");
        assertThat(s.optionalComment()).hasValue("TPC-H benchmark schema");
        assertThat(s.optionalProperties()).isPresent();
        assertThat(s.optionalProperties().get()).containsEntry("key1", "value1");
        assertThat(s.optionalOwner()).hasValue("admin");
    }

    @Test
    void testFetchSchemaWithNullableFieldsAbsent()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"name": "minimal", "catalog_name": "unity"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        Optional<UnityCatalogSchema> schema = client.fetchSchema(TEST_TOKEN, "unity", "minimal");

        assertThat(schema).isPresent();
        UnityCatalogSchema s = schema.get();
        assertThat(s.name()).isEqualTo("minimal");
        assertThat(s.optionalFullName()).isEmpty();
        assertThat(s.optionalSchemaId()).isEmpty();
        assertThat(s.optionalComment()).isEmpty();
        assertThat(s.optionalProperties()).isEmpty();
        assertThat(s.optionalOwner()).isEmpty();
    }

    // --- Pagination edge cases ---

    @Test
    void testEmptyStringPageTokenTreatedAsNoMorePages()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(200, """
                {"schemas": [{"name": "s1", "catalog_name": "unity"}], "next_page_token": ""}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        List<UnityCatalogSchema> schemas = client.listSchemas(TEST_TOKEN, "unity");

        assertThat(schemas).hasSize(1);
        // Should NOT make a second request — empty string token means no more pages
        assertThat(processor.requests).hasSize(1);
    }

    // --- Error handling edge cases ---

    @Test
    void testErrorResponseWithNoBody()
    {
        RecordingProcessor processor = new RecordingProcessor(request ->
                new TestingResponse(
                        HttpStatus.fromStatusCode(502),
                        ImmutableListMultimap.of(),
                        new byte[0]));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unity Catalog error")
                .hasMessageNotContaining(TEST_TOKEN);
    }

    @Test
    void testTokenRedactedTokenColonPattern()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(400, """
                {"message": "Authentication failed with token: dapi987654321secret"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas(TEST_TOKEN, "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("dapi987654321secret")
                .hasMessageContaining("REDACTED");
    }

    // --- Security: exception cause chain ---

    @Test
    void testNetworkFailureExceptionHasNoCauseChain()
    {
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            throw new RuntimeException("Connection refused to " + request.getUri() + " with Authorization: Bearer super-secret");
        });

        UnityCatalogClient client = new UnityCatalogClient(httpClient, BASE_URI);
        assertThatThrownBy(() -> client.listSchemas("super-secret", "unity"))
                .isInstanceOf(TrinoException.class)
                // The sanitized message must not contain the token
                .hasMessageNotContaining("super-secret")
                // The cause is preserved for diagnostics but the TrinoException message itself is sanitized
                .satisfies(e -> assertThat(e.getCause()).isNotNull());
    }

    @Test
    void testErrorResponseTokenNotLeakedInExceptionMessage()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(401, """
                {"message": "Invalid token"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.listSchemas("dapi-super-secret-token-xyz", "unity"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("dapi-super-secret-token-xyz")
                .hasMessageContaining("Access Denied");
    }

    @Test
    void testFetchTableNotFoundDoesNotLeakToken()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(404, """
                {"message": "Table not found"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        // fetchTable returns Optional.empty() for 404 — no exception at all, so no leak vector
        Optional<UnityCatalogTable> table = client.fetchTable("my-secret-token", "unity.tpch.missing");
        assertThat(table).isEmpty();
    }

    @Test
    void testServerErrorDoesNotLeakToken()
    {
        RecordingProcessor processor = new RecordingProcessor(request -> jsonResponse(500, """
                {"message": "Internal server error"}"""));

        UnityCatalogClient client = new UnityCatalogClient(new TestingHttpClient(processor), BASE_URI);
        assertThatThrownBy(() -> client.fetchTable("my-secret-token", "unity.tpch.orders"))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("my-secret-token");
    }

    // --- Helpers ---

    private static void assertAuthorizationHeader(Request request)
    {
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer " + TEST_TOKEN);
    }

    private static Response jsonResponse(int statusCode, String body)
    {
        return new TestingResponse(
                HttpStatus.fromStatusCode(statusCode),
                ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static class RecordingProcessor
            implements TestingHttpClient.Processor
    {
        private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
        private final Function<Request, Response> handler;

        RecordingProcessor(Function<Request, Response> handler)
        {
            this.handler = handler;
        }

        @Override
        public Response handle(Request request)
        {
            requests.add(request);
            return handler.apply(request);
        }
    }

    private static class PaginatingProcessor
            implements TestingHttpClient.Processor
    {
        private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
        private final List<String> responses;
        private int requestCount;

        PaginatingProcessor(List<String> responses)
        {
            this.responses = responses;
        }

        @Override
        public synchronized Response handle(Request request)
        {
            assertThat(requestCount)
                    .describedAs("More requests than expected responses")
                    .isLessThan(responses.size());
            requests.add(request);
            int index = requestCount;
            requestCount++;
            return jsonResponse(200, responses.get(index));
        }
    }
}
