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

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogTokenProvider;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;

public class UnityCatalogVendedCredentialsProvider
        implements VendedCredentialsProvider
{
    private static final int CACHE_MAX_SIZE = 1000;
    private static final Duration CACHE_EXPIRY = Duration.ofHours(1);

    private final UnityCatalogClient client;
    private final UnityCatalogTokenProvider tokenProvider;
    private final Clock clock;

    private final Cache<CacheKey, VendedCredentialsHandle> credentialsCache;

    @Inject
    public UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider)
    {
        this(client, tokenProvider, Clock.systemUTC());
    }

    UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider, Clock clock)
    {
        this.client = requireNonNull(client, "client is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.credentialsCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(CACHE_MAX_SIZE)
                .expireAfterWrite(CACHE_EXPIRY)
                .build();
    }

    @Override
    public VendedCredentialsHandle getFreshCredentials(ConnectorSession session, VendedCredentialsHandle handle)
    {
        if (!handle.catalogOwned() || handle.tableId().isEmpty()) {
            return handle;
        }

        String userId = session.getIdentity().getUser();
        CacheKey cacheKey = new CacheKey(userId, handle.tableLocation(), handle.tableId().get());

        // Invalidate expired entries so the loader fetches fresh credentials
        VendedCredentialsHandle cached = credentialsCache.getIfPresent(cacheKey);
        if (cached != null && cached.vendedCredentials().isPresent() && !cached.vendedCredentials().get().isValid()) {
            credentialsCache.invalidate(cacheKey);
        }

        VendedCredentialsHandle result = uncheckedCacheGet(credentialsCache, cacheKey, () -> fetchCredentials(session, handle));

        // Guard against freshly-fetched credentials that are already near expiry
        if (result.vendedCredentials().isPresent() && !result.vendedCredentials().get().isValid()) {
            credentialsCache.invalidate(cacheKey);
            result = uncheckedCacheGet(credentialsCache, cacheKey, () -> fetchCredentials(session, handle));
        }
        return result;
    }

    private VendedCredentialsHandle fetchCredentials(ConnectorSession session, VendedCredentialsHandle handle)
    {
        String token = tokenProvider.token(session.getIdentity());
        TemporaryCredentials temporaryCredentials = client.generateTemporaryTableCredentials(
                token,
                handle.tableId().get(),
                // TODO: Thread operation type (READ vs READ_WRITE) through VendedCredentialsHandle
                //  to support least-privilege credential vending for write operations
                "READ");

        FileSystemCredentials fileSystemCredentials = new UnityCatalogFileSystemCredentials(temporaryCredentials, clock);
        return new VendedCredentialsHandle(
                handle.catalogOwned(),
                handle.managed(),
                handle.tableLocation(),
                handle.tableId(),
                Optional.of(fileSystemCredentials));
    }

    private record CacheKey(String userId, String tableLocation, String tableId) {}
}
