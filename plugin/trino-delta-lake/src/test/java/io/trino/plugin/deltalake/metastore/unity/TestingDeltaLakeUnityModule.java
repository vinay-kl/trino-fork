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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogConfig;
import io.trino.unity.UnityCatalogTokenProvider;

import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Testing replacement for {@link DeltaLakeUnityMetastoreModule} that accepts
 * pre-constructed instances instead of creating real HTTP clients via {@link io.trino.unity.UnityCatalogModule}.
 */
final class TestingDeltaLakeUnityModule
        implements Module
{
    private final UnityCatalogClient client;
    private final UnityCatalogTokenProvider tokenProvider;

    TestingDeltaLakeUnityModule(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider)
    {
        this.client = requireNonNull(client, "client is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(UnityCatalogConfig.class);
        binder.bind(UnityCatalogClient.class).toInstance(client);
        binder.bind(UnityCatalogTokenProvider.class).toInstance(tokenProvider);
        newOptionalBinder(binder, DeltaLakeMetastoreFactory.class)
                .setBinding().to(UnityCatalogDeltaLakeMetastoreFactory.class).in(Scopes.SINGLETON);
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeUnityTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(0);

        // Unity Catalog does not use Hive metastore — satisfy DeltaLakeModule's default OptionalBinder
        binder.bind(HiveMetastoreFactory.class).toInstance(new HiveMetastoreFactory()
        {
            @Override
            public boolean isImpersonationEnabled()
            {
                return false;
            }

            @Override
            public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
            {
                throw new UnsupportedOperationException("HiveMetastore is not available with Unity Catalog");
            }
        });
        binder.bind(new TypeLiteral<Optional<CachingHiveMetastore>>() {}).toInstance(Optional.empty());
    }
}
