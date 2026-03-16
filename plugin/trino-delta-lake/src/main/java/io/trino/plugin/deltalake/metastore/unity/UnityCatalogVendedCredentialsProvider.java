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
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.unity.TemporaryCredentials;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogConfig;
import io.trino.unity.UnityCatalogTokenProvider;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;

public class UnityCatalogVendedCredentialsProvider
        implements VendedCredentialsProvider
{
    private static final Logger log = Logger.get(UnityCatalogVendedCredentialsProvider.class);

    private static final int CACHE_MAX_SIZE = 5000;
    private static final Duration CACHE_EXPIRY = Duration.ofHours(1);

    // Query-scoped cache for write credentials — coalesces all per-split calls within the same query
    // into a single UC API call. Keyed by (queryId, userId, tableId, operationType) so different
    // queries always get fresh credentials. Entries become unreachable after the query finishes;
    // TTL matches STS credential lifetime for cleanup.
    private static final int WRITE_CACHE_MAX_SIZE = 500;
    private static final Duration WRITE_CACHE_EXPIRY = Duration.ofHours(1);

    private final UnityCatalogClient client;
    private final UnityCatalogTokenProvider tokenProvider;
    private final Clock clock;
    private final boolean bypassCredentialCacheOnWrite;

    private final Cache<CacheKey, VendedCredentialsHandle> credentialsCache;
    private final Cache<WriteCacheKey, VendedCredentialsHandle> writeCredentialsCache;

    @Inject
    public UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider, UnityCatalogConfig config)
    {
        this(client, tokenProvider, Clock.systemUTC(), config.isBypassCredentialCacheOnWrite());
    }

    UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider)
    {
        this(client, tokenProvider, Clock.systemUTC(), true);
    }

    UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider, Clock clock)
    {
        this(client, tokenProvider, clock, true);
    }

    UnityCatalogVendedCredentialsProvider(UnityCatalogClient client, UnityCatalogTokenProvider tokenProvider, Clock clock, boolean bypassCredentialCacheOnWrite)
    {
        this.client = requireNonNull(client, "client is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.bypassCredentialCacheOnWrite = bypassCredentialCacheOnWrite;
        this.credentialsCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(CACHE_MAX_SIZE)
                .expireAfterWrite(CACHE_EXPIRY)
                .build();
        this.writeCredentialsCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(WRITE_CACHE_MAX_SIZE)
                .expireAfterWrite(WRITE_CACHE_EXPIRY)
                .build();
    }

    @Override
    public VendedCredentialsHandle getFreshCredentials(ConnectorSession session, VendedCredentialsHandle handle)
    {
        if (!handle.catalogOwned()) {
            return handle;
        }

        // Path-based credentials for table creation (CTAS / CREATE TABLE) — table doesn't exist yet, no tableId
        if (handle.tableId().isEmpty()) {
            if (handle.operationType().equals(VendedCredentialsHandle.PATH_CREATE_TABLE)) {
                log.debug("Fetching path credentials for %s", handle.tableLocation());
                return fetchPathCredentials(session, handle);
            }
            return handle;
        }

        String userId = session.getIdentity().getUser();
        CacheKey cacheKey = new CacheKey(userId, handle.tableLocation(), handle.tableId().get(), handle.operationType());

        // Write operations use a query-scoped cache: keyed by queryId so each query gets fresh credentials,
        // but all splits within the same query share one credential set. This is critical for scalability —
        // queries can have millions of splits, each creating a page sink that needs a filesystem.
        if (bypassCredentialCacheOnWrite && VendedCredentialsHandle.READ_WRITE.equals(handle.operationType())) {
            WriteCacheKey writeCacheKey = new WriteCacheKey(session.getQueryId(), userId, handle.tableId().get(), handle.operationType());
            VendedCredentialsHandle result = uncheckedCacheGet(writeCredentialsCache, writeCacheKey, () -> {
                log.debug("Write credential cache MISS — calling UC API for queryId=%s, table=%s", session.getQueryId(), handle.tableLocation());
                return fetchCredentials(session, handle);
            });
            // Guard against cached credentials that expired during a long-running query
            if (result.vendedCredentials().isPresent() && !result.vendedCredentials().get().isValid()) {
                writeCredentialsCache.invalidate(writeCacheKey);
                log.debug("Write credentials expired — refreshing for queryId=%s, table=%s", session.getQueryId(), handle.tableLocation());
                result = uncheckedCacheGet(writeCredentialsCache, writeCacheKey, () -> fetchCredentials(session, handle));
            }
            return result;
        }

        // Invalidate expired entries so the loader fetches fresh credentials
        VendedCredentialsHandle cached = credentialsCache.getIfPresent(cacheKey);
        if (cached != null && cached.vendedCredentials().isPresent() && !cached.vendedCredentials().get().isValid()) {
            credentialsCache.invalidate(cacheKey);
        }

        VendedCredentialsHandle result = uncheckedCacheGet(credentialsCache, cacheKey, () -> {
            log.debug("Read credential cache MISS — calling UC API for operationType=%s, table=%s", handle.operationType(), handle.tableLocation());
            return fetchCredentials(session, handle);
        });

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
        long startNanos = System.nanoTime();
        TemporaryCredentials temporaryCredentials = client.generateTemporaryTableCredentials(
                token,
                handle.tableId().get(),
                handle.operationType());
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        log.debug("UC API generateTemporaryTableCredentials took %dms for operationType=%s, table=%s", elapsedMs, handle.operationType(), handle.tableLocation());

        FileSystemCredentials fileSystemCredentials = new UnityCatalogFileSystemCredentials(temporaryCredentials, clock);
        return new VendedCredentialsHandle(
                handle.catalogOwned(),
                handle.managed(),
                handle.tableLocation(),
                handle.tableId(),
                handle.operationType(),
                Optional.of(fileSystemCredentials));
    }

    private VendedCredentialsHandle fetchPathCredentials(ConnectorSession session, VendedCredentialsHandle handle)
    {
        String token = tokenProvider.token(session.getIdentity());
        long startNanos = System.nanoTime();
        TemporaryCredentials temporaryCredentials = client.generateTemporaryPathCredentials(
                token,
                handle.tableLocation(),
                handle.operationType());
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        log.debug("UC API generateTemporaryPathCredentials took %dms for operation=%s, path=%s", elapsedMs, handle.operationType(), handle.tableLocation());

        FileSystemCredentials fileSystemCredentials = new UnityCatalogFileSystemCredentials(temporaryCredentials, clock);
        return new VendedCredentialsHandle(
                handle.catalogOwned(),
                handle.managed(),
                handle.tableLocation(),
                handle.tableId(),
                handle.operationType(),
                Optional.of(fileSystemCredentials));
    }

    @Override
    public void queryCompleted(String queryId)
    {
        log.debug("queryCompleted: evicting write credential cache entries for queryId=%s", queryId);
        writeCredentialsCache.asMap().keySet().removeIf(key -> key.queryId().equals(queryId));
    }

    private record CacheKey(String userId, String tableLocation, String tableId, String operationType) {}

    private record WriteCacheKey(String queryId, String userId, String tableId, String operationType) {}
}
