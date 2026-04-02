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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreType.THRIFT;
import static io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreType.UNITY;

final class TestDeltaLakeMetastoreTypeConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DeltaLakeMetastoreTypeConfig.class)
                .setMetastoreType(THRIFT));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of("hive.metastore", "UNITY");
        assertFullMapping(properties, new DeltaLakeMetastoreTypeConfig()
                .setMetastoreType(UNITY));
    }
}
