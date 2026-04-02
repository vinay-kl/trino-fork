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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UnityCatalogClient
{
    private static final Logger log = Logger.get(UnityCatalogClient.class);
    private static final int MAX_PAGINATION_PAGES = 1000;

    private final HttpClient httpClient;
    private final URI baseUri;
    private final JsonCodec<JsonNode> jsonCodec;
    private final ObjectMapper objectMapper;

    public UnityCatalogClient(HttpClient httpClient, URI baseUri)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.baseUri = requireNonNull(baseUri, "baseUri is null");
        this.jsonCodec = JsonCodec.jsonCodec(JsonNode.class);
        this.objectMapper = new ObjectMapper();
    }

    public List<UnityCatalogSchema> listSchemas(String token, String catalogName)
    {
        ImmutableList.Builder<UnityCatalogSchema> allSchemas = ImmutableList.builder();
        String pageToken = null;
        int pageCount = 0;
        do {
            HttpUriBuilder uriBuilder = uriBuilderFrom(baseUri)
                    .appendPath("/schemas")
                    .addParameter("catalog_name", catalogName)
                    .addParameter("max_results", "100");
            if (pageToken != null) {
                uriBuilder.addParameter("page_token", pageToken);
            }
            JsonNode response = executeGet(uriBuilder.build(), token);
            JsonNode schemasNode = response.path("schemas");
            if (schemasNode.isArray()) {
                for (JsonNode schemaNode : schemasNode) {
                    allSchemas.add(deserialize(schemaNode, UnityCatalogSchema.class));
                }
            }
            pageToken = extractPageToken(response);
            pageCount++;
            if (pageCount >= MAX_PAGINATION_PAGES) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unity Catalog pagination exceeded %d pages for schemas in catalog %s", MAX_PAGINATION_PAGES, catalogName));
            }
        }
        while (pageToken != null);
        return allSchemas.build();
    }

    public Optional<UnityCatalogSchema> fetchSchema(String token, String catalogName, String schemaName)
    {
        URI uri = uriBuilderFrom(baseUri)
                .appendPath("/schemas")
                .appendPath(format("%s.%s", catalogName, schemaName))
                .build();
        try {
            JsonNode response = executeGet(uri, token);
            return Optional.of(deserialize(response, UnityCatalogSchema.class));
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }
    }

    public UnityCatalogSchema createSchema(String token, String catalogName, String schemaName, Optional<String> comment)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/schemas").build();
        ImmutableMap.Builder<String, Object> body = ImmutableMap.<String, Object>builder()
                .put("catalog_name", catalogName)
                .put("name", schemaName);
        comment.ifPresent(c -> body.put("comment", c));
        JsonNode response = executePost(uri, token, body.buildOrThrow());
        return deserialize(response, UnityCatalogSchema.class);
    }

    public void deleteSchema(String token, String catalogName, String schemaName)
    {
        URI uri = uriBuilderFrom(baseUri)
                .appendPath("/schemas")
                .appendPath(format("%s.%s", catalogName, schemaName))
                .build();
        executeDelete(uri, token);
    }

    public List<UnityCatalogTable> listTables(String token, String catalogName, String schemaName)
    {
        ImmutableList.Builder<UnityCatalogTable> allTables = ImmutableList.builder();
        String pageToken = null;
        int pageCount = 0;
        do {
            HttpUriBuilder uriBuilder = uriBuilderFrom(baseUri)
                    .appendPath("/tables")
                    .addParameter("catalog_name", catalogName)
                    .addParameter("schema_name", schemaName)
                    .addParameter("max_results", "100");
            if (pageToken != null) {
                uriBuilder.addParameter("page_token", pageToken);
            }
            JsonNode response = executeGet(uriBuilder.build(), token);
            JsonNode tablesNode = response.path("tables");
            if (tablesNode.isArray()) {
                for (JsonNode tableNode : tablesNode) {
                    allTables.add(deserialize(tableNode, UnityCatalogTable.class));
                }
            }
            pageToken = extractPageToken(response);
            pageCount++;
            if (pageCount >= MAX_PAGINATION_PAGES) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unity Catalog pagination exceeded %d pages for tables in %s.%s", MAX_PAGINATION_PAGES, catalogName, schemaName));
            }
        }
        while (pageToken != null);
        return allTables.build();
    }

    public Optional<UnityCatalogTable> fetchTable(String token, String fullTableName)
    {
        URI uri = uriBuilderFrom(baseUri)
                .appendPath("/tables")
                .appendPath(fullTableName)
                .build();
        try {
            JsonNode response = executeGet(uri, token);
            return Optional.of(deserialize(response, UnityCatalogTable.class));
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }
    }

    public UnityCatalogTable createTable(String token, Map<String, Object> tableRequest)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/tables").build();
        JsonNode response = executePost(uri, token, tableRequest);
        return deserialize(response, UnityCatalogTable.class);
    }

    public void deleteTable(String token, String fullTableName)
    {
        URI uri = uriBuilderFrom(baseUri)
                .appendPath("/tables")
                .appendPath(fullTableName)
                .build();
        executeDelete(uri, token);
    }

    public List<String> fetchEffectivePermissions(String token, String securableType, String fullName)
    {
        URI uri = uriBuilderFrom(baseUri)
                .appendPath("/effective-permissions")
                .appendPath(securableType)
                .appendPath(fullName)
                .build();
        try {
            long startNanos = System.nanoTime();
            JsonNode response = executeGet(uri, token);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            log.debug("UC API fetchEffectivePermissions took %dms for %s %s", elapsedMs, securableType, fullName);
            JsonNode privilegeAssignments = response.path("privilege_assignments");
            if (!privilegeAssignments.isArray()) {
                return ImmutableList.of();
            }
            ImmutableList.Builder<String> privileges = ImmutableList.builder();
            for (JsonNode assignment : privilegeAssignments) {
                JsonNode privilegeNode = assignment.path("privileges");
                if (privilegeNode.isArray()) {
                    for (JsonNode effectivePrivilege : privilegeNode) {
                        // Databricks UC returns EffectivePrivilege objects: {"privilege": "SELECT", ...}
                        // OSS UC may return plain strings
                        JsonNode privilegeName = effectivePrivilege.path("privilege");
                        if (privilegeName.isTextual()) {
                            privileges.add(privilegeName.asText());
                        }
                        else if (effectivePrivilege.isTextual()) {
                            privileges.add(effectivePrivilege.asText());
                        }
                    }
                }
            }
            return privileges.build();
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                return ImmutableList.of();
            }
            throw e;
        }
    }

    public TemporaryCredentials generateTemporaryTableCredentials(String token, String tableId, String operation)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/temporary-table-credentials").build();
        long startNanos = System.nanoTime();
        JsonNode response = executePost(uri, token, ImmutableMap.of(
                "table_id", tableId,
                "operation", operation));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        log.debug("UC API generateTemporaryTableCredentials took %dms for tableId=%s, operation=%s", elapsedMs, tableId, operation);
        return deserialize(response, TemporaryCredentials.class);
    }

    public TemporaryCredentials generateTemporaryPathCredentials(String token, String url, String operation)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/temporary-path-credentials").build();
        long startNanos = System.nanoTime();
        JsonNode response = executePost(uri, token, ImmutableMap.of(
                "url", url,
                "operation", operation));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        log.debug("UC API generateTemporaryPathCredentials took %dms for operation=%s", elapsedMs, operation);
        return deserialize(response, TemporaryCredentials.class);
    }

    private JsonNode executeGet(URI uri, String token)
    {
        Request request = prepareGet()
                .setUri(uri)
                .addHeader(AUTHORIZATION, "Bearer " + token)
                .build();
        return execute(request);
    }

    private JsonNode executePost(URI uri, String token, Map<String, ?> body)
    {
        Request request = preparePost()
                .setUri(uri)
                .addHeader(AUTHORIZATION, "Bearer " + token)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(jsonCodec, serializeToJsonNode(body)))
                .build();
        return execute(request);
    }

    private void executeDelete(URI uri, String token)
    {
        Request request = prepareDelete()
                .setUri(uri)
                .addHeader(AUTHORIZATION, "Bearer " + token)
                .build();
        execute(request);
    }

    private static final int MAX_RETRIES = 3;
    private static final long[] RETRY_DELAYS_MS = {1000, 2000, 4000};

    private JsonNode execute(Request request)
    {
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            FullJsonResponseHandler.JsonResponse<JsonNode> response;
            try {
                response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec));
            }
            catch (RuntimeException e) {
                // Strip token from exception messages (UC-SEC-004)
                throw sanitizedException(format("Unity Catalog request failed: %s", request.getUri().getPath()), e);
            }
            int statusCode = response.getStatusCode();
            if (HttpStatus.familyForStatusCode(statusCode) == HttpStatus.Family.SUCCESSFUL) {
                if (!response.hasValue()) {
                    // Some successful responses (e.g., DELETE) may not have a body
                    return objectMapper.createObjectNode();
                }
                return response.getValue();
            }
            if (statusCode == 429 && attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(RETRY_DELAYS_MS[attempt]);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Interrupted during Unity Catalog retry");
                }
                continue;
            }
            handleErrorResponse(statusCode, request.getUri().getPath(), response);
        }
        // unreachable — handleErrorResponse always throws
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected error");
    }

    private void handleErrorResponse(int statusCode, String path, FullJsonResponseHandler.JsonResponse<JsonNode> response)
    {
        // Extract error message from response body if available, but never include tokens
        String errorMessage = extractSafeErrorMessage(response);
        String resource = extractResourceName(path);
        switch (statusCode) {
            case 401, 403 -> throw new TrinoException(PERMISSION_DENIED,
                    format("Access Denied%s", formatDetail(errorMessage)));
            case 404 -> throw new TrinoException(NOT_FOUND,
                    format("Unity Catalog resource not found: %s", resource));
            case 409 -> throw new TrinoException(ALREADY_EXISTS,
                    format("Unity Catalog resource already exists: %s%s", resource, formatDetail(errorMessage)));
            case 429 -> throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unity Catalog rate limit exceeded for %s", resource));
            default -> throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unity Catalog error (HTTP %d) for %s%s", statusCode, resource, formatDetail(errorMessage)));
        }
    }

    private static String extractResourceName(String path)
    {
        // Extract the meaningful resource from the API path, e.g.
        // "/api/2.1/unity-catalog/tables/prod.adm.my_table" → "table prod.adm.my_table"
        // "/api/2.1/unity-catalog/schemas/prod.adm" → "schema prod.adm"
        if (path == null) {
            return "unknown resource";
        }
        int tablesIndex = path.lastIndexOf("/tables/");
        if (tablesIndex >= 0) {
            return "table " + path.substring(tablesIndex + "/tables/".length());
        }
        int schemasIndex = path.lastIndexOf("/schemas/");
        if (schemasIndex >= 0) {
            return "schema " + path.substring(schemasIndex + "/schemas/".length());
        }
        int catalogsIndex = path.lastIndexOf("/catalogs/");
        if (catalogsIndex >= 0) {
            return "catalog " + path.substring(catalogsIndex + "/catalogs/".length());
        }
        // For other paths (e.g. credential vending), show the last path segment
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < path.length() - 1) {
            return path.substring(lastSlash + 1);
        }
        return path;
    }

    private static String formatDetail(String errorMessage)
    {
        if (errorMessage.isEmpty()) {
            return "";
        }
        return ": " + errorMessage;
    }

    private String extractSafeErrorMessage(FullJsonResponseHandler.JsonResponse<JsonNode> response)
    {
        if (!response.hasValue()) {
            return "";
        }
        JsonNode body = response.getValue();
        // UC error responses typically have a "message" field
        JsonNode messageNode = body.path("message");
        if (messageNode.isTextual()) {
            String message = messageNode.asText();
            // Redact any token-like strings in the error message (UC-SEC-004)
            return redactTokens(message);
        }
        return "";
    }

    private static String redactTokens(String message)
    {
        // Redact Bearer tokens and common credential patterns
        return message.replaceAll("(?i)(bearer\\s+)[\\w\\-.~+/]+=*", "$1[REDACTED]")
                .replaceAll("(?i)(token[=:]\\s*)[\\w\\-.~+/]+=*", "$1[REDACTED]");
    }

    private String extractPageToken(JsonNode response)
    {
        JsonNode tokenNode = response.path("next_page_token");
        if (tokenNode.isTextual() && !tokenNode.asText().isEmpty()) {
            return tokenNode.asText();
        }
        return null;
    }

    private <T> T deserialize(JsonNode node, Class<T> type)
    {
        try {
            return objectMapper.treeToValue(node, type);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Failed to deserialize Unity Catalog response as %s", type.getSimpleName()), e);
        }
    }

    private JsonNode serializeToJsonNode(Map<String, ?> map)
    {
        return objectMapper.valueToTree(map);
    }

    private static TrinoException sanitizedException(String message, Throwable cause)
    {
        // Preserve the cause for stack trace diagnostics, but sanitize the message
        // to avoid leaking Authorization headers or token values (UC-SEC-004)
        log.debug(cause, "Unity Catalog REST call failed: %s", message);
        return new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
    }
}
