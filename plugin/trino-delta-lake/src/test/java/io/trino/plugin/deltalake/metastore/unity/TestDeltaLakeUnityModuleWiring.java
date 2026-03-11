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
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.unity.ExtraCredentialsTokenProvider;
import io.trino.unity.StaticTokenProvider;
import io.trino.unity.UnityCatalogSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
final class TestDeltaLakeUnityModuleWiring
{
    private static final String UC_CATALOG_NAME = "unity_test";

    @Test
    void testUnityModuleBindsAndStartsSuccessfully()
            throws Exception
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(new UnityCatalogSchema(
                "test_schema",
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + ".test_schema",
                "schema-id-1",
                null,
                null,
                null));

        StaticTokenProvider tokenProvider = new StaticTokenProvider("test-token");

        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(DELTA_CATALOG)
                                .setSchema("test_schema")
                                .build())
                .build()) {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new TestingDeltaLakePlugin(
                    dataDir,
                    () -> Optional.of(new TestingDeltaLakeUnityModule(client, tokenProvider))));

            queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("unity-catalog.server-uri", "http://localhost:0")
                    .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                    .put("unity-catalog.auth-type", "STATIC")
                    .put("unity-catalog.static-token", "test-token")
                    .put("unity-catalog.allow-http-endpoint", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .buildOrThrow());

            // Verify the catalog is operational — SHOW SCHEMAS exercises the full binding chain
            assertThat(queryRunner.execute("SHOW SCHEMAS FROM delta").getRowCount())
                    .isGreaterThan(0);
        }
    }

    @Test
    void testUnityModuleWithExtraCredentialsAuth()
            throws Exception
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.addSchema(new UnityCatalogSchema(
                "test_schema",
                UC_CATALOG_NAME,
                UC_CATALOG_NAME + ".test_schema",
                "schema-id-1",
                null,
                null,
                null));

        ExtraCredentialsTokenProvider tokenProvider = new ExtraCredentialsTokenProvider(
                "unity-catalog.token",
                Optional.empty());

        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(DELTA_CATALOG)
                                .setSchema("test_schema")
                                .build())
                .build()) {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new TestingDeltaLakePlugin(
                    dataDir,
                    () -> Optional.of(new TestingDeltaLakeUnityModule(client, tokenProvider))));

            queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("unity-catalog.server-uri", "http://localhost:0")
                    .put("unity-catalog.catalog-name", UC_CATALOG_NAME)
                    .put("unity-catalog.auth-type", "EXTRA_CREDENTIALS")
                    .put("unity-catalog.allow-http-endpoint", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .buildOrThrow());
        }
    }
}
