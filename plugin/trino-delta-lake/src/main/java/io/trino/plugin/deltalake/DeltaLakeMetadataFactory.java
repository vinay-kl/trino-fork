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

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreFactory.DeltaLakeMetastores;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.FileBasedTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.spi.Node;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class DeltaLakeMetadataFactory
{
    private final DeltaLakeMetastoreFactory metastoreFactory;
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final Node currentNode;
    private final CheckpointWriterManager checkpointWriterManager;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final int domainCompactionThreshold;
    private final boolean unsafeWritesEnabled;
    private final long checkpointWritingInterval;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean useUniqueTableLocation;
    private final DeltaLakeTableMetadataScheduler metadataScheduler;
    private final Executor metadataFetchingExecutor;
    private final boolean allowManagedTableRename;
    private final VendedCredentialsProvider vendedCredentialsProvider;
    private final TransactionLogReaderFactory transactionLogReaderFactory;

    @Inject
    public DeltaLakeMetadataFactory(
            DeltaLakeMetastoreFactory metastoreFactory,
            DeltaLakeFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager,
            DeltaLakeConfig deltaLakeConfig,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            Node currentNode,
            CheckpointWriterManager checkpointWriterManager,
            CachingExtendedStatisticsAccess statisticsAccess,
            @AllowDeltaLakeManagedTableRename boolean allowManagedTableRename,
            VendedCredentialsProvider vendedCredentialsProvider,
            DeltaLakeTableMetadataScheduler metadataScheduler,
            @ForDeltaLakeMetadata ExecutorService executorService,
            TransactionLogReaderFactory transactionLogReaderFactory)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.checkpointWriterManager = requireNonNull(checkpointWriterManager, "checkpointWriterManager is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.unsafeWritesEnabled = deltaLakeConfig.getUnsafeWritesEnabled();
        this.checkpointWritingInterval = deltaLakeConfig.getDefaultCheckpointWritingInterval();
        this.deleteSchemaLocationsFallback = deltaLakeConfig.isDeleteSchemaLocationsFallback();
        this.useUniqueTableLocation = deltaLakeConfig.isUniqueTableLocation();
        this.allowManagedTableRename = allowManagedTableRename;
        this.vendedCredentialsProvider = requireNonNull(vendedCredentialsProvider, "vendedCredentialsProvider is null");
        this.metadataScheduler = requireNonNull(metadataScheduler, "metadataScheduler is null");
        if (deltaLakeConfig.getMetadataParallelism() == 1) {
            this.metadataFetchingExecutor = directExecutor();
        }
        else {
            this.metadataFetchingExecutor = new BoundedExecutor(executorService, deltaLakeConfig.getMetadataParallelism());
        }
        this.transactionLogReaderFactory = requireNonNull(transactionLogReaderFactory, "transactionLogLoaderFactory is null");
    }

    public DeltaLakeMetadata create(ConnectorIdentity identity)
    {
        DeltaLakeMetastores metastores = metastoreFactory.createMetastores(identity);
        FileBasedTableStatisticsProvider tableStatisticsProvider = new FileBasedTableStatisticsProvider(
                typeManager,
                transactionLogAccess,
                statisticsAccess);
        return new DeltaLakeMetadata(
                metastores.metastore(),
                transactionLogAccess,
                tableStatisticsProvider,
                fileSystemFactory,
                typeManager,
                metastores.viewMetastore(),
                domainCompactionThreshold,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                transactionLogWriterFactory,
                currentNode,
                checkpointWriterManager,
                checkpointWritingInterval,
                deleteSchemaLocationsFallback,
                statisticsAccess,
                metadataScheduler,
                useUniqueTableLocation,
                allowManagedTableRename,
                vendedCredentialsProvider,
                metadataFetchingExecutor,
                transactionLogReaderFactory);
    }
}
