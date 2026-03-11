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
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.spi.TrinoException;
import io.trino.unity.TemporaryCredentials;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

final class UnityCatalogFileSystemCredentials
        implements FileSystemCredentials
{
    private static final Duration EXPIRY_BUFFER = Duration.ofMinutes(5);
    // Mirrors GcsStorageFactory.GCS_OAUTH_KEY — cannot import directly due to no module dependency
    private static final String GCS_OAUTH_KEY = "gcs.oauth";

    private final TemporaryCredentials credentials;
    private final Instant expirationTime;
    private final Clock clock;

    UnityCatalogFileSystemCredentials(TemporaryCredentials credentials, Clock clock)
    {
        this.credentials = requireNonNull(credentials, "credentials is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.expirationTime = resolveExpirationTime(credentials);
    }

    @Override
    public Map<String, String> asExtraCredentials()
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
        credentials.awsTempCredentials().ifPresent(aws -> {
            result.put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, aws.accessKeyId());
            result.put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, aws.secretAccessKey());
            result.put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, aws.sessionToken());
        });
        credentials.gcpTempCredentials().ifPresent(gcp ->
                result.put(GCS_OAUTH_KEY, gcp.oauthToken()));
        if (credentials.azureTempCredentials().isPresent() && credentials.awsTempCredentials().isEmpty() && credentials.gcpTempCredentials().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Azure credential vending via Unity Catalog is not yet supported");
        }
        return result.buildOrThrow();
    }

    @Override
    public boolean isValid()
    {
        return Instant.now(clock).plus(EXPIRY_BUFFER).isBefore(expirationTime);
    }

    private static Instant resolveExpirationTime(TemporaryCredentials credentials)
    {
        // Use the earliest expiration across all credential types and the top-level expiration.
        // Databricks UC returns expiration_time at the top level, while per-cloud credential
        // objects may or may not include their own expiration_time.
        return Stream.of(
                        credentials.expirationTime().map(UnityCatalogFileSystemCredentials::parseExpirationTime),
                        credentials.awsTempCredentials().map(aws -> parseExpirationTime(aws.expirationTime())),
                        credentials.gcpTempCredentials().map(gcp -> parseExpirationTime(gcp.expirationTime())),
                        credentials.azureTempCredentials().map(azure -> parseExpirationTime(azure.expirationTime())))
                .flatMap(Optional::stream)
                .min(Instant::compareTo)
                .orElse(Instant.MAX);
    }

    private static Instant parseExpirationTime(String expirationTime)
    {
        if (expirationTime == null || expirationTime.isEmpty()) {
            return Instant.MAX;
        }
        // UC returns epoch milliseconds as a string
        try {
            return Instant.ofEpochMilli(Long.parseLong(expirationTime));
        }
        catch (NumberFormatException e) {
            // Fall back to ISO-8601 format
            return Instant.parse(expirationTime);
        }
    }
}
