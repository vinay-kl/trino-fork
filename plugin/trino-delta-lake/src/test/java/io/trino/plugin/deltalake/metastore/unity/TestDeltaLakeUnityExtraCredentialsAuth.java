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
import io.trino.Session;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.unity.ExtraCredentialsTokenProvider;
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
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Required: tests assert on shared lastReceivedToken which would race under concurrent execution
final class TestDeltaLakeUnityExtraCredentialsAuth
{
    private static final String UC_CATALOG_NAME = "unity_test";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "invariants";

    private DistributedQueryRunner queryRunner;
    private TestingUnityCatalogClient client;

    @BeforeAll
    void createQueryRunner()
            throws Exception
    {
        client = new TestingUnityCatalogClient();

        // Pre-populate UC client with schema and table
        client.addSchema(new UnityCatalogSchema(
                TEST_SCHEMA,
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + "." + TEST_SCHEMA,
                "schema-id-1",
                null,
                null,
                null));

        // Build query runner first to get the data directory
        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(DELTA_CATALOG)
                                .setSchema(TEST_SCHEMA)
                                .build())
                .build();

        // Copy Delta table data into the runner's data directory
        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        Path tableDir = dataDir.resolve(TEST_SCHEMA).resolve(TEST_TABLE);
        Files.createDirectories(tableDir);
        Path sourceDir = new File(Resources.getResource("deltalake/invariants").toURI()).toPath();
        copyDirectoryContents(sourceDir, tableDir);

        // Register the table in UC client with local:/// path
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

        // Token provider: EXTRA_CREDENTIALS with no fallback
        ExtraCredentialsTokenProvider tokenProvider = new ExtraCredentialsTokenProvider(
                "unity-catalog.token",
                Optional.empty());

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new TestingDeltaLakePlugin(
                dataDir,
                Optional.of(new TestingDeltaLakeUnityModule(client, tokenProvider))));

        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                .put("unity-catalog.server-uri", "http://localhost:0")
                .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                .put("unity-catalog.auth-type", "EXTRA_CREDENTIALS")
                .put("unity-catalog.allow-http-endpoint", "true")
                    .put("unity-catalog.allow-loopback-endpoint", "true")
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
    void testShowSchemasWithValidToken()
    {
        Session session = sessionWithToken("alice", "alice-token");
        MaterializedResult result = queryRunner.execute(session, "SHOW SCHEMAS FROM delta");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(TEST_SCHEMA);
    }

    @Test
    void testShowTablesWithValidToken()
    {
        Session session = sessionWithToken("alice", "alice-token");
        MaterializedResult result = queryRunner.execute(session, "SHOW TABLES FROM " + TEST_SCHEMA);
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .contains(TEST_TABLE);
    }

    @Test
    void testSelectFromDeltaTableWithValidToken()
    {
        Session session = sessionWithToken("alice", "alice-token");
        MaterializedResult result = queryRunner.execute(session,
                "SELECT * FROM " + TEST_SCHEMA + "." + TEST_TABLE);
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
    }

    @Test
    void testQueryWithoutTokenFailsWithPermissionDenied()
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser("alice"))
                .build();

        assertThatThrownBy(() -> queryRunner.execute(session, "SHOW SCHEMAS FROM delta"))
                .hasMessageContaining("unity-catalog.token");
    }

    @Test
    void testQueryWithWrongCredentialKeyFails()
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser("alice")
                        .withExtraCredentials(ImmutableMap.of("wrong.credential.key", "some-token"))
                        .build())
                .build();

        // The token provider looks for "unity-catalog.token", not "wrong.credential.key"
        assertThatThrownBy(() -> queryRunner.execute(session, "SHOW SCHEMAS FROM delta"))
                .hasMessageContaining("unity-catalog.token");
    }

    @Test
    void testTokenForwardedPerUser()
    {
        Session aliceSession = sessionWithToken("alice", "alice-token");
        queryRunner.execute(aliceSession, "SHOW SCHEMAS FROM delta");
        assertThat(client.lastReceivedToken()).isEqualTo("alice-token");

        Session bobSession = sessionWithToken("bob", "bob-token");
        queryRunner.execute(bobSession, "SHOW SCHEMAS FROM delta");
        assertThat(client.lastReceivedToken()).isEqualTo("bob-token");
    }

    @Test
    void testCustomCredentialKeyName()
            throws Exception
    {
        TestingUnityCatalogClient customClient = new TestingUnityCatalogClient();
        customClient.addSchema(new UnityCatalogSchema(
                TEST_SCHEMA,
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + "." + TEST_SCHEMA,
                "schema-id-1",
                null,
                null,
                null));

        String customKeyName = "my-custom-uc-token";
        ExtraCredentialsTokenProvider customTokenProvider = new ExtraCredentialsTokenProvider(
                customKeyName,
                Optional.empty());

        try (DistributedQueryRunner customRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(DELTA_CATALOG)
                                .setSchema(TEST_SCHEMA)
                                .build())
                .build()) {
            Path dataDir = customRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");

            customRunner.installPlugin(new TpchPlugin());
            customRunner.createCatalog("tpch", "tpch");

            customRunner.installPlugin(new TestingDeltaLakePlugin(
                    dataDir,
                    Optional.of(new TestingDeltaLakeUnityModule(customClient, customTokenProvider))));

            customRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("unity-catalog.server-uri", "http://localhost:0")
                    .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                    .put("unity-catalog.auth-type", "EXTRA_CREDENTIALS")
                    .put("unity-catalog.extra-credential-name", customKeyName)
                    .put("unity-catalog.allow-http-endpoint", "true")
                    .put("unity-catalog.allow-loopback-endpoint", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .buildOrThrow());

            // Custom key should work
            Session validSession = testSessionBuilder()
                    .setCatalog(DELTA_CATALOG)
                    .setSchema(TEST_SCHEMA)
                    .setIdentity(Identity.forUser("alice")
                            .withExtraCredentials(ImmutableMap.of(customKeyName, "alice-token"))
                            .build())
                    .build();
            MaterializedResult result = customRunner.execute(validSession, "SHOW SCHEMAS FROM delta");
            assertThat(result.getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0)))
                    .contains(TEST_SCHEMA);

            // Default key should fail
            Session defaultKeySession = testSessionBuilder()
                    .setCatalog(DELTA_CATALOG)
                    .setSchema(TEST_SCHEMA)
                    .setIdentity(Identity.forUser("alice")
                            .withExtraCredentials(ImmutableMap.of("unity-catalog.token", "alice-token"))
                            .build())
                    .build();
            assertThatThrownBy(() -> customRunner.execute(defaultKeySession, "SHOW SCHEMAS FROM delta"))
                    .hasMessageContaining(customKeyName);
        }
    }

    private Session sessionWithToken(String user, String token)
    {
        return testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(user)
                        .withExtraCredentials(ImmutableMap.of("unity-catalog.token", token))
                        .build())
                .build();
    }
}
