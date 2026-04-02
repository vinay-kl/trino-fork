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

import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogConfig;
import io.trino.unity.UnityCatalogTokenProvider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class UnityCatalogDeltaLakeMetastoreFactory
        implements DeltaLakeMetastoreFactory
{
    private final UnityCatalogClient client;
    private final UnityCatalogTokenProvider tokenProvider;
    private final String catalogName;

    @Inject
    public UnityCatalogDeltaLakeMetastoreFactory(
            UnityCatalogClient client,
            UnityCatalogTokenProvider tokenProvider,
            UnityCatalogConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        requireNonNull(config, "config is null");
        this.catalogName = config.getCatalogName();
    }

    @Override
    public DeltaLakeMetastores createMetastores(ConnectorIdentity identity)
    {
        String token = tokenProvider.token(identity);
        UnityCatalogDeltaLakeMetastore metastore = new UnityCatalogDeltaLakeMetastore(client, token, catalogName);
        return new DeltaLakeMetastores(metastore, Optional.empty());
    }
}
