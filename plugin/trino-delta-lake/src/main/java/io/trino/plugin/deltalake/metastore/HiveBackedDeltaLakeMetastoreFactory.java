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

import com.google.inject.Inject;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.spi.NodeVersion;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;

public class HiveBackedDeltaLakeMetastoreFactory
        implements DeltaLakeMetastoreFactory
{
    private final HiveMetastoreFactory hiveMetastoreFactory;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final boolean usingSystemSecurity;
    private final String trinoVersion;

    @Inject
    public HiveBackedDeltaLakeMetastoreFactory(
            HiveMetastoreFactory hiveMetastoreFactory,
            DeltaLakeConfig deltaLakeConfig,
            @UsingSystemSecurity boolean usingSystemSecurity,
            NodeVersion nodeVersion)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
        requireNonNull(deltaLakeConfig, "deltaLakeConfig is null");
        this.perTransactionMetastoreCacheMaximumSize = deltaLakeConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.usingSystemSecurity = usingSystemSecurity;
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    @Override
    public DeltaLakeMetastores createMetastores(ConnectorIdentity identity)
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(
                hiveMetastoreFactory.createMetastore(Optional.of(identity)),
                perTransactionMetastoreCacheMaximumSize);
        HiveMetastoreBackedDeltaLakeMetastore deltaLakeMetastore = new HiveMetastoreBackedDeltaLakeMetastore(cachingHiveMetastore);
        TrinoViewHiveMetastore trinoViewHiveMetastore = new TrinoViewHiveMetastore(
                cachingHiveMetastore,
                usingSystemSecurity,
                trinoVersion,
                "Trino Delta Lake connector");
        return new DeltaLakeMetastores(deltaLakeMetastore, Optional.of(trinoViewHiveMetastore));
    }
}
