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

import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDeltaLakeUnityTableOperations
{
    @Test
    void testCommitIsNoOp()
    {
        DeltaLakeUnityTableOperations operations = new DeltaLakeUnityTableOperations();
        // Should complete without error — Unity Catalog reads schema directly from Delta log
        operations.commitToExistingTable(
                new SchemaTableName("schema", "table"),
                42L,
                "{\"type\":\"struct\",\"fields\":[]}",
                Optional.of("test comment"));
    }

    @Test
    void testCommitWithEmptyComment()
    {
        DeltaLakeUnityTableOperations operations = new DeltaLakeUnityTableOperations();
        operations.commitToExistingTable(
                new SchemaTableName("schema", "table"),
                1L,
                "{}",
                Optional.empty());
    }

    @Test
    void testProviderCreatesCorrectType()
    {
        DeltaLakeUnityTableOperationsProvider provider = new DeltaLakeUnityTableOperationsProvider();
        DeltaLakeTableOperations operations = provider.createTableOperations(null);
        assertThat(operations).isInstanceOf(DeltaLakeUnityTableOperations.class);
    }
}
