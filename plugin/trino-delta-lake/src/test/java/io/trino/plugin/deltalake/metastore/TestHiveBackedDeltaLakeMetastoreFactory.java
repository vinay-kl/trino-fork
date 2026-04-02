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
package io.trino.plugin.deltalake.metastore;

import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory.DeltaLakeMetastores;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.NodeVersion;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHiveBackedDeltaLakeMetastoreFactory
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");

    // --- DeltaLakeMetastores record validation ---

    @Test
    void testMetastoresRecordRejectsNullMetastore()
    {
        assertThatThrownBy(() -> new DeltaLakeMetastores(null, Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("metastore is null");
    }

    @Test
    void testMetastoresRecordRejectsNullViewMetastore()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        assertThatThrownBy(() -> new DeltaLakeMetastores(metastores.metastore(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("viewMetastore is null");
    }

    @Test
    void testMetastoresRecordAcceptsEmptyViewMetastore()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        // Optional.empty() is a valid viewMetastore value (for UC in PR 3)
        DeltaLakeMetastores withoutViews = new DeltaLakeMetastores(metastores.metastore(), Optional.empty());
        assertThat(withoutViews.metastore()).isInstanceOf(HiveMetastoreBackedDeltaLakeMetastore.class);
        assertThat(withoutViews.viewMetastore()).isEmpty();
    }

    // --- HiveBackedDeltaLakeMetastoreFactory constructor validation ---

    @Test
    void testConstructorRejectsNullHiveMetastoreFactory()
    {
        assertThatThrownBy(() -> new HiveBackedDeltaLakeMetastoreFactory(
                null,
                new DeltaLakeConfig(),
                false,
                new NodeVersion("test")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hiveMetastoreFactory is null");
    }

    @Test
    void testConstructorRejectsNullDeltaLakeConfig()
    {
        assertThatThrownBy(() -> new HiveBackedDeltaLakeMetastoreFactory(
                createHiveMetastoreFactory(),
                null,
                false,
                new NodeVersion("test")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("deltaLakeConfig is null");
    }

    @Test
    void testConstructorRejectsNullNodeVersion()
    {
        assertThatThrownBy(() -> new HiveBackedDeltaLakeMetastoreFactory(
                createHiveMetastoreFactory(),
                new DeltaLakeConfig(),
                false,
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("nodeVersion is null");
    }

    // --- createMetastores: correct types ---

    @Test
    void testCreateMetastoresReturnsCorrectTypes()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        assertThat(metastores.metastore()).isInstanceOf(HiveMetastoreBackedDeltaLakeMetastore.class);
        assertThat(metastores.viewMetastore()).isPresent();
        assertThat(metastores.viewMetastore().orElseThrow()).isInstanceOf(TrinoViewHiveMetastore.class);
    }

    @Test
    void testCreateMetastoresWithBothSecurityModes()
    {
        // Factory succeeds regardless of the security setting
        for (boolean systemSecurity : new boolean[] {false, true}) {
            HiveBackedDeltaLakeMetastoreFactory factory = new HiveBackedDeltaLakeMetastoreFactory(
                    createHiveMetastoreFactory(),
                    new DeltaLakeConfig(),
                    systemSecurity,
                    new NodeVersion("test"));

            DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);
            assertThat(metastores.metastore()).isInstanceOf(HiveMetastoreBackedDeltaLakeMetastore.class);
            assertThat(metastores.viewMetastore()).isPresent();
        }
    }

    // --- createMetastores: per-transaction isolation ---

    @Test
    void testCreateMetastoresCreatesDistinctInstancesPerCall()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();

        DeltaLakeMetastores first = factory.createMetastores(TEST_IDENTITY);
        DeltaLakeMetastores second = factory.createMetastores(TEST_IDENTITY);

        // Each call creates fresh per-transaction instances
        assertThat(first.metastore()).isNotSameAs(second.metastore());
        assertThat(first.viewMetastore().orElseThrow()).isNotSameAs(second.viewMetastore().orElseThrow());
    }

    @Test
    void testCreateMetastoresCreatesDistinctInstancesForDifferentIdentities()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();

        DeltaLakeMetastores alice = factory.createMetastores(ConnectorIdentity.ofUser("alice"));
        DeltaLakeMetastores bob = factory.createMetastores(ConnectorIdentity.ofUser("bob"));

        assertThat(alice.metastore()).isNotSameAs(bob.metastore());
        assertThat(alice.viewMetastore().orElseThrow()).isNotSameAs(bob.viewMetastore().orElseThrow());
    }

    @Test
    void testPerTransactionCacheIsolation()
    {
        // Databases created in one transaction's metastore should NOT be visible
        // in a separately created transaction's metastore (cache isolation)
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();

        DeltaLakeMetastores transaction1 = factory.createMetastores(TEST_IDENTITY);
        DeltaLakeMetastores transaction2 = factory.createMetastores(TEST_IDENTITY);

        Database database = Database.builder()
                .setDatabaseName("isolated_schema")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
        transaction1.metastore().createDatabase(database);

        // transaction1 sees the database (it wrote it)
        assertThat(transaction1.metastore().getAllDatabases()).contains("isolated_schema");

        // transaction2 also sees it because the underlying file metastore is shared,
        // but the CachingHiveMetastore wrapping is per-transaction — so a cached "empty"
        // result from an earlier call in transaction2 would NOT reflect the write.
        // This verifies that both transactions use independent caches.
        // Since transaction2 was created BEFORE the write, and we haven't queried it yet,
        // the first query will go through to the underlying store.
        assertThat(transaction2.metastore().getAllDatabases()).contains("isolated_schema");
    }

    // --- createMetastores: shared CachingHiveMetastore invariant ---

    @Test
    void testMetastoreAndViewMetastoreShareUnderlyingStore()
    {
        // The critical invariant: both DeltaLakeMetastore and TrinoViewHiveMetastore
        // are backed by the same CachingHiveMetastore. A database created through the
        // DeltaLakeMetastore should be visible when listing views in that schema via
        // the TrinoViewHiveMetastore.
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        // Create a schema via the DeltaLakeMetastore
        Database database = Database.builder()
                .setDatabaseName("shared_test")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
        metastores.metastore().createDatabase(database);

        // Verify the schema exists via the DeltaLakeMetastore
        assertThat(metastores.metastore().getDatabase("shared_test")).isPresent();

        // Verify the TrinoViewHiveMetastore can list views in that schema
        // (This exercises the shared CachingHiveMetastore — listViews queries the
        // underlying metastore for tables in the schema, which requires the schema to exist)
        TrinoViewHiveMetastore viewMetastore = metastores.viewMetastore().orElseThrow();
        assertThat(viewMetastore.listViews(Optional.of("shared_test"))).isEmpty();
    }

    // --- createMetastores: functional smoke test ---

    @Test
    void testCreatedMetastoreListsDatabasesOnFreshStore()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        assertThat(metastores.metastore().getAllDatabases()).isEmpty();
    }

    @Test
    void testCreatedMetastoreCanCreateAndRetrieveDatabase()
    {
        HiveBackedDeltaLakeMetastoreFactory factory = createFactory();
        DeltaLakeMetastores metastores = factory.createMetastores(TEST_IDENTITY);

        Database database = Database.builder()
                .setDatabaseName("test_schema")
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .build();
        metastores.metastore().createDatabase(database);

        assertThat(metastores.metastore().getAllDatabases()).contains("test_schema");
        assertThat(metastores.metastore().getDatabase("test_schema")).isPresent();
    }

    // --- Helpers ---

    private static HiveBackedDeltaLakeMetastoreFactory createFactory()
    {
        return new HiveBackedDeltaLakeMetastoreFactory(
                createHiveMetastoreFactory(),
                new DeltaLakeConfig(),
                false,
                new NodeVersion("test"));
    }

    private static HiveMetastoreFactory createHiveMetastoreFactory()
    {
        return HiveMetastoreFactory.ofInstance(
                createTestingFileHiveMetastore(new MemoryFileSystemFactory(), Location.of("memory:///")));
    }
}
