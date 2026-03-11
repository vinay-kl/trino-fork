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

import io.trino.spi.TrinoException;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.TemporaryCredentials.AwsTempCredentials;
import io.trino.unity.TemporaryCredentials.AzureTempCredentials;
import io.trino.unity.TemporaryCredentials.GcpTempCredentials;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogFileSystemCredentials
{
    @Test
    void testAwsExtraCredentials()
    {
        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", "9999999999999");
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        Map<String, String> extra = fsCredentials.asExtraCredentials();

        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "AKID");
        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "secret");
        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "token");
        assertThat(extra).hasSize(3);
    }

    @Test
    void testGcsExtraCredentials()
    {
        GcpTempCredentials gcp = new GcpTempCredentials("gcp-oauth-token", "9999999999999");
        TemporaryCredentials credentials = new TemporaryCredentials(null, null, gcp, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        Map<String, String> extra = fsCredentials.asExtraCredentials();

        assertThat(extra).containsEntry("gcs.oauth", "gcp-oauth-token");
        assertThat(extra).hasSize(1);
    }

    @Test
    void testAzureOnlyThrowsNotSupported()
    {
        AzureTempCredentials azure = new AzureTempCredentials("sas-token", "9999999999999");
        TemporaryCredentials credentials = new TemporaryCredentials(null, azure, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        assertThatThrownBy(fsCredentials::asExtraCredentials)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Azure credential vending")
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()));
    }

    @Test
    void testIsValidBeforeExpiry()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationEpochMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", expirationEpochMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isTrue();
    }

    @Test
    void testIsInvalidAfterExpiry()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // Expired 1 hour ago
        String expirationEpochMs = String.valueOf(now.minus(Duration.ofHours(1)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", expirationEpochMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testIsInvalidNearExpiry()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // Expires in 3 minutes — less than the 5-minute buffer
        String expirationEpochMs = String.valueOf(now.plus(Duration.ofMinutes(3)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", expirationEpochMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testIsValidWithNoExpirationTime()
    {
        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", null);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        assertThat(fsCredentials.isValid()).isTrue();
    }

    @Test
    void testIsValidWithEmptyCredentials()
    {
        TemporaryCredentials credentials = new TemporaryCredentials(null, null, null, null);
        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        assertThat(fsCredentials.isValid()).isTrue();
        assertThat(fsCredentials.asExtraCredentials()).isEmpty();
    }

    @Test
    void testIsValidWithIso8601ExpirationFormat()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // ISO-8601 format expiration — 1 hour from now
        String iso8601Expiration = "2025-01-01T13:00:00Z";

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", iso8601Expiration);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isTrue();
    }

    @Test
    void testIsInvalidWithIso8601PastExpiration()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String iso8601Expiration = "2025-01-01T11:00:00Z";

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", iso8601Expiration);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testAzureWithAwsDoesNotThrowAndEmitsAwsCredentials()
    {
        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", "9999999999999");
        AzureTempCredentials azure = new AzureTempCredentials("sas-token", "9999999999999");
        TemporaryCredentials credentials = new TemporaryCredentials(aws, azure, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        Map<String, String> extra = fsCredentials.asExtraCredentials();

        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "AKID");
        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "secret");
        assertThat(extra).containsEntry(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "token");
        assertThat(extra).hasSize(3);
    }

    @Test
    void testAzureWithGcpDoesNotThrowAndEmitsGcpCredentials()
    {
        GcpTempCredentials gcp = new GcpTempCredentials("gcp-oauth-token", "9999999999999");
        AzureTempCredentials azure = new AzureTempCredentials("sas-token", "9999999999999");
        TemporaryCredentials credentials = new TemporaryCredentials(null, azure, gcp, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        Map<String, String> extra = fsCredentials.asExtraCredentials();

        assertThat(extra).containsEntry("gcs.oauth", "gcp-oauth-token");
        assertThat(extra).hasSize(1);
    }

    @Test
    void testEarliestExpiryAcrossMultipleCredentialTypes()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // AWS expires in 1 hour, GCP expires in 30 minutes — GCP should govern validity
        String awsExpirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());
        String gcpExpirationMs = String.valueOf(now.plus(Duration.ofMinutes(30)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", awsExpirationMs);
        GcpTempCredentials gcp = new GcpTempCredentials("gcp-oauth-token", gcpExpirationMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, gcp, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isTrue();

        // Now test with GCP expiring within the 5-minute buffer — should be invalid
        String nearGcpExpirationMs = String.valueOf(now.plus(Duration.ofMinutes(3)).toEpochMilli());
        GcpTempCredentials nearExpiryGcp = new GcpTempCredentials("gcp-oauth-token", nearGcpExpirationMs);
        TemporaryCredentials nearExpiryCredentials = new TemporaryCredentials(aws, null, nearExpiryGcp, null);

        UnityCatalogFileSystemCredentials nearExpiryFsCredentials = new UnityCatalogFileSystemCredentials(nearExpiryCredentials, clock);
        // Even though AWS is valid for 1 hour, GCP's near-expiry should make the whole credential invalid
        assertThat(nearExpiryFsCredentials.isValid()).isFalse();
    }

    @Test
    void testIsValidWithEmptyStringExpirationTime()
    {
        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", "");
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC());
        assertThat(fsCredentials.isValid()).isTrue();
    }

    @Test
    void testInvalidExpirationTimeFormatThrows()
    {
        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", "not-a-date");
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        assertThatThrownBy(() -> new UnityCatalogFileSystemCredentials(credentials, Clock.systemUTC()))
                .isInstanceOf(DateTimeException.class);
    }

    @Test
    void testTopLevelExpirationTimeGovernsValidity()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // AWS per-cloud expiration is far future, but top-level expiration is near-expiry (3 min)
        String farExpirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());
        String nearExpirationMs = String.valueOf(now.plus(Duration.ofMinutes(3)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", farExpirationMs);
        // Top-level expiration (4th arg) is the Databricks pattern — epoch millis as a number
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, nearExpirationMs);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        // Top-level near-expiry should make credentials invalid despite per-cloud being valid
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testTopLevelExpirationTimeOnlyDatabricksPattern()
    {
        // Databricks returns expiration_time at top level only, not inside aws_temp_credentials
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String topLevelExpirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", null);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, topLevelExpirationMs);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isTrue();
    }

    @Test
    void testTopLevelExpirationTimeOnlyExpired()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String topLevelExpirationMs = String.valueOf(now.minus(Duration.ofHours(1)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", null);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, topLevelExpirationMs);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testIsInvalidAtExactExpiryBoundary()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // Expiration is exactly now + 5 minutes (the buffer) — isBefore returns false
        String expirationEpochMs = String.valueOf(now.plus(Duration.ofMinutes(5)).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", expirationEpochMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isFalse();
    }

    @Test
    void testIsValidJustBeyondExpiryBuffer()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        // Expiration is 5 minutes + 1 millisecond — just past the buffer
        String expirationEpochMs = String.valueOf(now.plus(Duration.ofMinutes(5)).plusMillis(1).toEpochMilli());

        AwsTempCredentials aws = new AwsTempCredentials("AKID", "secret", "token", expirationEpochMs);
        TemporaryCredentials credentials = new TemporaryCredentials(aws, null, null, null);

        UnityCatalogFileSystemCredentials fsCredentials = new UnityCatalogFileSystemCredentials(credentials, clock);
        assertThat(fsCredentials.isValid()).isTrue();
    }
}
