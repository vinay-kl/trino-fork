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
package io.trino.plugin.deltalake;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

public interface DeltaLakeFileSystemFactory
        extends TrinoFileSystemFactory
{
    default TrinoFileSystem create(ConnectorSession session, DeltaLakeTableHandle table)
    {
        return create(session, table.toCredentialsHandle());
    }

    default TrinoFileSystem create(ConnectorSession session, DeltaMetastoreTable table)
    {
        return create(session, VendedCredentialsHandle.of(table));
    }

    TrinoFileSystem create(ConnectorSession session, VendedCredentialsHandle table);

    /**
     * For external table create/write using location
     */
    TrinoFileSystem create(ConnectorSession session, String tableLocation);

    /**
     * Fallback for code paths that don't have table-specific credential context.
     * Creates a file system using the session identity without vended credentials.
     * Prefer {@link #create(ConnectorSession, VendedCredentialsHandle)} or
     * {@link #create(ConnectorSession, DeltaLakeTableHandle)} when a table handle is available.
     */
    @Override
    default TrinoFileSystem create(ConnectorSession session)
    {
        return create(session, VendedCredentialsHandle.empty(""));
    }

    /**
     * Fallback for code paths that only have a ConnectorIdentity.
     * Cannot apply credential vending without a session. Implementations should
     * override this to delegate to the underlying TrinoFileSystemFactory.
     */
    @Override
    TrinoFileSystem create(ConnectorIdentity identity);
}
