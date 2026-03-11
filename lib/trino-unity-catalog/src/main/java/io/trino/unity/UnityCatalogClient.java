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
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UnityCatalogClient
{
    private static final Logger log = Logger.get(UnityCatalogClient.class);

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

    public TemporaryCredentials generateTemporaryTableCredentials(String token, String tableId, String operation)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/temporary-table-credentials").build();
        JsonNode response = executePost(uri, token, ImmutableMap.of(
                "table_id", tableId,
                "operation", operation));
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

    private JsonNode execute(Request request)
    {
        FullJsonResponseHandler.JsonResponse<JsonNode> response;
        try {
            response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec));
        }
        catch (RuntimeException e) {
            // Strip token from exception messages (UC-SEC-004)
            throw sanitizedException("Unity Catalog request failed: %s".formatted(request.getUri().getPath()), e);
        }
        int statusCode = response.getStatusCode();
        if (HttpStatus.familyForStatusCode(statusCode) == HttpStatus.Family.SUCCESSFUL) {
            if (!response.hasValue()) {
                // Some successful responses (e.g., DELETE) may not have a body
                return objectMapper.createObjectNode();
            }
            return response.getValue();
        }
        handleErrorResponse(statusCode, request.getUri().getPath(), response);
        // unreachable — handleErrorResponse always throws
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected error");
    }

    private void handleErrorResponse(int statusCode, String path, FullJsonResponseHandler.JsonResponse<JsonNode> response)
    {
        // Extract error message from response body if available, but never include tokens
        String errorMessage = extractSafeErrorMessage(response);
        switch (statusCode) {
            case 401, 403 -> throw new TrinoException(PERMISSION_DENIED,
                    format("Unity Catalog access denied for %s: %s", path, errorMessage));
            case 404 -> throw new TrinoException(NOT_FOUND,
                    format("Unity Catalog resource not found: %s", path));
            case 409 -> throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unity Catalog conflict for %s: %s", path, errorMessage));
            case 429 -> throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unity Catalog rate limit exceeded for %s", path));
            default -> throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unity Catalog error (HTTP %d) for %s: %s", statusCode, path, errorMessage));
        }
    }

    private String extractSafeErrorMessage(FullJsonResponseHandler.JsonResponse<JsonNode> response)
    {
        if (!response.hasValue()) {
            return "no response body";
        }
        JsonNode body = response.getValue();
        // UC error responses typically have a "message" field
        JsonNode messageNode = body.path("message");
        if (messageNode.isTextual()) {
            String message = messageNode.asText();
            // Redact any token-like strings in the error message (UC-SEC-004)
            return redactTokens(message);
        }
        return "status %d".formatted(response.getStatusCode());
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
                    "Failed to deserialize Unity Catalog response as %s".formatted(type.getSimpleName()), e);
        }
    }

    private JsonNode serializeToJsonNode(Map<String, ?> map)
    {
        return objectMapper.valueToTree(map);
    }

    private static TrinoException sanitizedException(String message, Throwable cause)
    {
        // Log only the exception class and sanitized message — never the full cause chain,
        // which may contain Authorization headers or token values (UC-SEC-004)
        log.debug("Unity Catalog REST call failed (%s): %s", cause.getClass().getSimpleName(), message);
        return new TrinoException(GENERIC_INTERNAL_ERROR, message);
    }
}
