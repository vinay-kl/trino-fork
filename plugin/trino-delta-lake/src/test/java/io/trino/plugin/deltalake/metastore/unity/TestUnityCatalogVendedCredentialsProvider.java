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

import io.airlift.json.JsonCodec;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.TemporaryCredentials.AwsTempCredentials;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogVendedCredentialsProvider
{
    private static final String TOKEN = "test-token";

    @Test
    void testFreshCredentials()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result.vendedCredentials()).isPresent();
        assertThat(result.vendedCredentials().get().asExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "AKID");
        assertThat(result.vendedCredentials().get().isValid()).isTrue();
    }

    @Test
    void testCachedCredentials()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle first = provider.getFreshCredentials(session, handle);
        VendedCredentialsHandle second = provider.getFreshCredentials(session, handle);

        // Same cached credentials should be returned
        assertThat(first.vendedCredentials().get()).isSameAs(second.vendedCredentials().get());
        // Client should only be called once — second call should hit cache
        assertThat(client.credentialVendingCallCount()).isEqualTo(1);
    }

    @Test
    void testExpiredCredentialsRefreshed()
    {
        // Start at a time where credentials are about to expire (within 5-min buffer)
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        String nearExpirationMs = String.valueOf(now.plus(Duration.ofMinutes(3)).toEpochMilli());
        String farExpirationMs = String.valueOf(now.plus(Duration.ofHours(2)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        // First call returns near-expiry credentials
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID1", "secret1", "session1", nearExpirationMs),
                null,
                null,
                null));

        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        // First call: fetches near-expiry creds, detects invalid, retries (still near-expiry from same client)
        VendedCredentialsHandle first = provider.getFreshCredentials(session, handle);
        assertThat(first.vendedCredentials()).isPresent();
        assertThat(first.vendedCredentials().get().isValid()).isFalse();
        // Two calls: initial fetch + retry after post-fetch validity check
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);

        // Update client to return fresh credentials
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID2", "secret2", "session2", farExpirationMs),
                null,
                null,
                null));

        // Second call should invalidate expired cache and fetch fresh credentials
        VendedCredentialsHandle second = provider.getFreshCredentials(session, handle);
        assertThat(second.vendedCredentials()).isPresent();
        assertThat(second.vendedCredentials().get().isValid()).isTrue();
        assertThat(second.vendedCredentials().get().asExtraCredentials())
                .containsEntry(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "AKID2");
        assertThat(client.credentialVendingCallCount()).isEqualTo(3);
    }

    @Test
    void testNonCatalogOwnedPassesThrough()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN);

        VendedCredentialsHandle handle = VendedCredentialsHandle.empty("s3://bucket/table");
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result).isSameAs(handle);
        assertThat(result.vendedCredentials()).isEmpty();
    }

    @Test
    void testMissingTableIdPassesThrough()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.empty(), VendedCredentialsHandle.READ, Optional.empty());
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result).isSameAs(handle);
    }

    @Test
    void testCacheKeyedByUserAndTable()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());

        ConnectorSession aliceSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();
        ConnectorSession bobSession = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("bob"))
                .build();

        VendedCredentialsHandle aliceResult = provider.getFreshCredentials(aliceSession, handle);
        VendedCredentialsHandle bobResult = provider.getFreshCredentials(bobSession, handle);

        // Different users should each trigger a separate credential vending call
        assertThat(aliceResult.vendedCredentials()).isPresent();
        assertThat(bobResult.vendedCredentials()).isPresent();
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);
    }

    @Test
    void testCacheKeyedByTableLocation()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle1 = new VendedCredentialsHandle(
                true, false, "s3://bucket/table-a", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());
        VendedCredentialsHandle handle2 = new VendedCredentialsHandle(
                true, false, "s3://bucket/table-b", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        provider.getFreshCredentials(session, handle1);
        provider.getFreshCredentials(session, handle2);

        // Same user and tableId but different tableLocation should be separate cache entries
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);
    }

    @Test
    void testCredentialVendingFailureDoesNotCache()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        // Credentials not configured — first call will throw
        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        assertThatThrownBy(() -> provider.getFreshCredentials(session, handle))
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);

        // After failure, configure credentials — retry should succeed (failure not cached)
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));
        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result.vendedCredentials()).isPresent();
        assertThat(result.vendedCredentials().get().isValid()).isTrue();
    }

    @Test
    void testCacheKeyedByOperationType()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle readHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());
        VendedCredentialsHandle writeHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ_WRITE, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        provider.getFreshCredentials(session, readHandle);
        provider.getFreshCredentials(session, writeHandle);

        // Different operationType should produce separate cache entries — two vending calls
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);

        // Verify the operation type was passed correctly for each call
        assertThat(client.operationsForTable("table-id-1")).contains(VendedCredentialsHandle.READ_WRITE);
    }

    @Test
    void testReadAndWriteCredentialsNotSharedInCache()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle readHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());
        VendedCredentialsHandle writeHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ_WRITE, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        // Fetch READ credentials first, then READ_WRITE
        VendedCredentialsHandle readResult = provider.getFreshCredentials(session, readHandle);
        VendedCredentialsHandle writeResult = provider.getFreshCredentials(session, writeHandle);

        // Both should have credentials
        assertThat(readResult.vendedCredentials()).isPresent();
        assertThat(writeResult.vendedCredentials()).isPresent();
        // Should be separate cache entries (2 calls total)
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);

        // Fetching READ again should hit cache (still 2 calls)
        provider.getFreshCredentials(session, readHandle);
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);

        // Fetching READ_WRITE again hits short-lived write cache (still 2 calls — coalesced for per-split reuse)
        provider.getFreshCredentials(session, writeHandle);
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);
    }

    @Test
    void testReadAndWriteCredentialsCachedWhenBypassDisabled()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock,
                false);

        VendedCredentialsHandle readHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ, Optional.empty());
        VendedCredentialsHandle writeHandle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ_WRITE, Optional.empty());

        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        provider.getFreshCredentials(session, readHandle);
        provider.getFreshCredentials(session, writeHandle);
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);

        // Both should hit cache when bypass is disabled
        provider.getFreshCredentials(session, readHandle);
        provider.getFreshCredentials(session, writeHandle);
        assertThat(client.credentialVendingCallCount()).isEqualTo(2);
    }

    @Test
    void testPathCreateTableRoutesToPathCredentials()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        // PATH_CREATE_TABLE with no tableId should route to path credentials
        VendedCredentialsHandle handle = VendedCredentialsHandle.forPathCreate("s3://bucket/new-table");
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result.vendedCredentials()).isPresent();
        assertThat(result.vendedCredentials().get().isValid()).isTrue();

        // Should have called path credentials, not table credentials
        assertThat(client.pathCredentialRequests()).hasSize(1);
        assertThat(client.pathCredentialRequests().getFirst().url()).isEqualTo("s3://bucket/new-table");
        assertThat(client.pathCredentialRequests().getFirst().operation()).isEqualTo("PATH_CREATE_TABLE");

        // Table credentials should NOT have been called
        assertThat(client.operationsForTable("table-id-1")).isEmpty();
    }

    @Test
    void testPathCredentialsNotCached()
    {
        Instant now = Instant.parse("2025-01-01T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        String expirationMs = String.valueOf(now.plus(Duration.ofHours(1)).toEpochMilli());

        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setTemporaryCredentials(new TemporaryCredentials(
                new AwsTempCredentials("AKID", "secret", "session", expirationMs),
                null,
                null,
                null));

        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN,
                clock);

        VendedCredentialsHandle handle = VendedCredentialsHandle.forPathCreate("s3://bucket/new-table");
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        // Call twice — each should make a fresh API call (no caching for path credentials)
        provider.getFreshCredentials(session, handle);
        provider.getFreshCredentials(session, handle);
        assertThat(client.pathCredentialRequests()).hasSize(2);
    }

    @Test
    void testCatalogOwnedWithEmptyTableIdAndReadPassesThrough()
    {
        // catalogOwned=true, tableId=empty, operationType=READ should pass through (not path credentials)
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogVendedCredentialsProvider provider = new UnityCatalogVendedCredentialsProvider(
                client,
                identity -> TOKEN);

        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.empty(), VendedCredentialsHandle.READ, Optional.empty());
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result).isSameAs(handle);
        assertThat(client.pathCredentialRequests()).isEmpty();
    }

    @Test
    void testSerializationBackwardCompatibility()
    {
        JsonCodec<VendedCredentialsHandle> codec = JsonCodec.jsonCodec(VendedCredentialsHandle.class);

        // Simulate old serialized handle without operationType field
        String jsonWithoutOperationType = """
                {"catalogOwned":true,"managed":false,"tableLocation":"s3://bucket/table","tableId":"table-id-1","vendedCredentials":null}""";
        VendedCredentialsHandle deserialized = codec.fromJson(jsonWithoutOperationType);

        // Should default to READ
        assertThat(deserialized.operationType()).isEqualTo(VendedCredentialsHandle.READ);
        assertThat(deserialized.catalogOwned()).isTrue();
        assertThat(deserialized.tableLocation()).isEqualTo("s3://bucket/table");
        assertThat(deserialized.tableId()).isPresent();
        assertThat(deserialized.tableId().get()).isEqualTo("table-id-1");
    }

    @Test
    void testSerializationRoundTrip()
    {
        JsonCodec<VendedCredentialsHandle> codec = JsonCodec.jsonCodec(VendedCredentialsHandle.class);

        VendedCredentialsHandle original = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id-1"), VendedCredentialsHandle.READ_WRITE, Optional.empty());
        String json = codec.toJson(original);
        VendedCredentialsHandle deserialized = codec.fromJson(json);

        assertThat(deserialized.catalogOwned()).isEqualTo(original.catalogOwned());
        assertThat(deserialized.managed()).isEqualTo(original.managed());
        assertThat(deserialized.tableLocation()).isEqualTo(original.tableLocation());
        assertThat(deserialized.tableId()).isEqualTo(original.tableId());
        assertThat(deserialized.operationType()).isEqualTo(VendedCredentialsHandle.READ_WRITE);
    }
}
