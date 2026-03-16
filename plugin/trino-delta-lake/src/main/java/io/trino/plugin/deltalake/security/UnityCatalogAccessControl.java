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
package io.trino.plugin.deltalake.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import io.trino.unity.UnityCatalogClient;
import io.trino.unity.UnityCatalogConfig;
import io.trino.unity.UnityCatalogTokenProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UnityCatalogAccessControl
        implements ConnectorAccessControl
{
    private static final Logger log = Logger.get(UnityCatalogAccessControl.class);

    private static final long DEFAULT_CACHE_TTL_MINUTES = 5;
    private static final long MAX_CACHE_SIZE = 50_000;

    private final UnityCatalogClient client;
    private final UnityCatalogTokenProvider tokenProvider;
    private final String catalogName;
    private final boolean bypassPermissionCacheOnWrite;
    private final com.google.common.cache.Cache<PermissionCacheKey, Set<String>> permissionCache;

    @Inject
    public UnityCatalogAccessControl(
            UnityCatalogClient client,
            UnityCatalogTokenProvider tokenProvider,
            UnityCatalogConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        this.catalogName = requireNonNull(config.getCatalogName(), "catalogName is null");
        this.bypassPermissionCacheOnWrite = config.isBypassPermissionCacheOnWrite();
        this.permissionCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(DEFAULT_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    private Set<String> getTablePrivileges(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        String fullName = format("%s.%s.%s", catalogName, tableName.getSchemaName(), tableName.getTableName());
        PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "TABLE", fullName);
        return uncheckedCacheGet(permissionCache, cacheKey, () -> {
            String token = tokenProvider.token(context.getIdentity());
            return ImmutableSet.copyOf(client.getEffectivePermissions(token, "TABLE", fullName));
        });
    }

    private Set<String> getSchemaPrivileges(ConnectorSecurityContext context, String schemaName)
    {
        String fullName = format("%s.%s", catalogName, schemaName);
        PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "SCHEMA", fullName);
        return uncheckedCacheGet(permissionCache, cacheKey, () -> {
            String token = tokenProvider.token(context.getIdentity());
            return ImmutableSet.copyOf(client.getEffectivePermissions(token, "SCHEMA", fullName));
        });
    }

    private boolean hasPrivilege(Set<String> privileges, String required)
    {
        return privileges.contains(required) || privileges.contains("ALL_PRIVILEGES");
    }

    private void checkTablePrivilege(ConnectorSecurityContext context, SchemaTableName tableName, String privilege, String operation)
    {
        Set<String> privileges = getTablePrivileges(context, tableName);
        if (!hasPrivilege(privileges, privilege)) {
            throw new AccessDeniedException(format("Access denied: %s on %s.%s requires %s privilege",
                    operation, tableName.getSchemaName(), tableName.getTableName(), privilege));
        }
    }

    private void checkSchemaPrivilege(ConnectorSecurityContext context, String schemaName, String privilege, String operation)
    {
        Set<String> privileges = getSchemaPrivileges(context, schemaName);
        if (!hasPrivilege(privileges, privilege)) {
            throw new AccessDeniedException(format("Access denied: %s on schema %s requires %s privilege",
                    operation, schemaName, privilege));
        }
    }

    // Write-path privilege checks — bypass cache when configured for fresh UC permission checks on every write.
    // Calls UC API directly (no cache lookup) to eliminate TOCTOU race between invalidate and reload.
    // Invalidates the stale entry afterward so subsequent read checks also see fresh data.
    private Set<String> fetchTablePrivilegesFresh(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        String fullName = format("%s.%s.%s", catalogName, tableName.getSchemaName(), tableName.getTableName());
        String token = tokenProvider.token(context.getIdentity());
        Set<String> privileges = ImmutableSet.copyOf(client.getEffectivePermissions(token, "TABLE", fullName));
        PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "TABLE", fullName);
        permissionCache.invalidate(cacheKey);
        return privileges;
    }

    private Set<String> fetchSchemaPrivilegesFresh(ConnectorSecurityContext context, String schemaName)
    {
        String fullName = format("%s.%s", catalogName, schemaName);
        String token = tokenProvider.token(context.getIdentity());
        Set<String> privileges = ImmutableSet.copyOf(client.getEffectivePermissions(token, "SCHEMA", fullName));
        PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "SCHEMA", fullName);
        permissionCache.invalidate(cacheKey);
        return privileges;
    }

    private Set<String> fetchCatalogPrivilegesFresh(ConnectorSecurityContext context)
    {
        String token = tokenProvider.token(context.getIdentity());
        Set<String> privileges = ImmutableSet.copyOf(client.getEffectivePermissions(token, "CATALOG", catalogName));
        PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "CATALOG", catalogName);
        permissionCache.invalidate(cacheKey);
        return privileges;
    }

    private void checkTablePrivilegeWrite(ConnectorSecurityContext context, SchemaTableName tableName, String privilege, String operation)
    {
        Set<String> privileges = bypassPermissionCacheOnWrite
                ? fetchTablePrivilegesFresh(context, tableName)
                : getTablePrivileges(context, tableName);
        if (!hasPrivilege(privileges, privilege)) {
            throw new AccessDeniedException(format("Access denied: %s on %s.%s requires %s privilege",
                    operation, tableName.getSchemaName(), tableName.getTableName(), privilege));
        }
    }

    private void checkSchemaPrivilegeWrite(ConnectorSecurityContext context, String schemaName, String privilege, String operation)
    {
        Set<String> privileges = bypassPermissionCacheOnWrite
                ? fetchSchemaPrivilegesFresh(context, schemaName)
                : getSchemaPrivileges(context, schemaName);
        if (!hasPrivilege(privileges, privilege)) {
            throw new AccessDeniedException(format("Access denied: %s on schema %s requires %s privilege",
                    operation, schemaName, privilege));
        }
    }

    // Schema operations

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName, Map<String, Object> properties)
    {
        // CREATE_SCHEMA is a catalog-level privilege — the schema doesn't exist yet,
        // so we check against the catalog securable instead
        Set<String> privileges;
        if (bypassPermissionCacheOnWrite) {
            privileges = fetchCatalogPrivilegesFresh(context);
        }
        else {
            PermissionCacheKey cacheKey = new PermissionCacheKey(context.getIdentity().getUser(), "CATALOG", catalogName);
            privileges = uncheckedCacheGet(permissionCache, cacheKey, () -> {
                String token = tokenProvider.token(context.getIdentity());
                return ImmutableSet.copyOf(client.getEffectivePermissions(token, "CATALOG", catalogName));
            });
        }
        if (!hasPrivilege(privileges, "CREATE_SCHEMA")) {
            throw new AccessDeniedException(format("Access denied: CREATE SCHEMA requires CREATE_SCHEMA privilege on catalog %s", catalogName));
        }
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        checkSchemaPrivilegeWrite(context, schemaName, "MANAGE", "DROP SCHEMA");
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        checkSchemaPrivilegeWrite(context, schemaName, "MANAGE", "RENAME SCHEMA");
    }

    @Override
    public void checkCanSetSchemaAuthorization(ConnectorSecurityContext context, String schemaName, TrinoPrincipal principal)
    {
        checkSchemaPrivilegeWrite(context, schemaName, "MANAGE", "SET SCHEMA AUTHORIZATION");
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context) {}

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(ConnectorSecurityContext context, String schemaName) {}

    // Table operations

    @Override
    public void checkCanShowCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilege(context, tableName, "SELECT", "SHOW CREATE TABLE");
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
    {
        checkSchemaPrivilegeWrite(context, tableName.getSchemaName(), "CREATE_TABLE", "CREATE TABLE");
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MANAGE", "DROP TABLE");
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MANAGE", "RENAME TABLE");
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "SET TABLE COMMENT");
    }

    @Override
    public void checkCanSetViewComment(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        checkTablePrivilegeWrite(context, viewName, "MODIFY", "SET VIEW COMMENT");
    }

    @Override
    public void checkCanSetTableProperties(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Optional<Object>> properties)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "SET TABLE PROPERTIES");
    }

    @Override
    public void checkCanSetColumnComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "SET COLUMN COMMENT");
    }

    @Override
    public void checkCanShowTables(ConnectorSecurityContext context, String schemaName) {}

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(ConnectorSecurityContext context, SchemaTableName table)
    {
        checkTablePrivilege(context, table, "SELECT", "SHOW COLUMNS");
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(ConnectorSecurityContext context, Map<SchemaTableName, Set<String>> tableColumns)
    {
        return tableColumns;
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "ADD COLUMN");
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "DROP COLUMN");
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "RENAME COLUMN");
    }

    @Override
    public void checkCanAlterColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "ALTER COLUMN");
    }

    @Override
    public void checkCanSetTableAuthorization(ConnectorSecurityContext context, SchemaTableName tableName, TrinoPrincipal principal)
    {
        checkTablePrivilegeWrite(context, tableName, "MANAGE", "SET TABLE AUTHORIZATION");
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        checkTablePrivilege(context, tableName, "SELECT", "SELECT");
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "INSERT");
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "DELETE");
    }

    @Override
    public void checkCanTruncateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "TRUNCATE");
    }

    @Override
    public void checkCanUpdateTableColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> updatedColumnNames)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "UPDATE");
    }

    // View operations — Delta Lake doesn't support views, deny all

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        throw new AccessDeniedException("Views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanRenameView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        throw new AccessDeniedException("Views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanRefreshView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        throw new AccessDeniedException("Views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanSetViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        throw new AccessDeniedException("Views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        throw new AccessDeniedException("Views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        checkTablePrivilege(context, tableName, "SELECT", "CREATE VIEW WITH SELECT");
    }

    // Materialized view operations — not supported

    @Override
    public void checkCanCreateMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Object> properties)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanRefreshMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanDropMaterializedView(ConnectorSecurityContext context, SchemaTableName materializedViewName)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanRenameMaterializedView(ConnectorSecurityContext context, SchemaTableName viewName, SchemaTableName newViewName)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanSetMaterializedViewAuthorization(ConnectorSecurityContext context, SchemaTableName viewName, TrinoPrincipal principal)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    @Override
    public void checkCanSetMaterializedViewProperties(ConnectorSecurityContext context, SchemaTableName materializedViewName, Map<String, Optional<Object>> properties)
    {
        throw new AccessDeniedException("Materialized views are not supported by Unity Catalog Delta Lake connector");
    }

    // Session properties — allow all

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName) {}

    // Grant/Revoke — not managed through Trino for UC

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanDenySchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanDenyTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    // Role operations — not supported

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        throw new AccessDeniedException("Role management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        throw new AccessDeniedException("Role management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new AccessDeniedException("Role management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new AccessDeniedException("Role management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role)
    {
        throw new AccessDeniedException("Role management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context) {}

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context) {}

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context) {}

    // Procedure execution

    @Override
    public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
    {
        String procedureName = procedure.getRoutineName();
        switch (procedureName) {
            case "register_table", "unregister_table" ->
                    checkSchemaPrivilegeWrite(context, procedure.getSchemaName(), "CREATE_TABLE", format("EXECUTE PROCEDURE %s", procedureName));
            case "flush_metadata_cache" -> {
                // Cache maintenance is not security-sensitive — allow for all authenticated users
            }
            default -> throw new AccessDeniedException(format("Execution of procedure '%s' is not permitted", procedureName));
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(ConnectorSecurityContext context, SchemaTableName tableName, String procedure)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "EXECUTE TABLE PROCEDURE");
    }

    // Function operations

    @Override
    public boolean canExecuteFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        return true;
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(ConnectorSecurityContext context, SchemaRoutineName function)
    {
        return true;
    }

    @Override
    public void checkCanShowFunctions(ConnectorSecurityContext context, String schemaName) {}

    @Override
    public Set<SchemaFunctionName> filterFunctions(ConnectorSecurityContext context, Set<SchemaFunctionName> functionNames)
    {
        return functionNames;
    }

    @Override
    public void checkCanCreateFunction(ConnectorSecurityContext context, SchemaRoutineName function) {}

    @Override
    public void checkCanDropFunction(ConnectorSecurityContext context, SchemaRoutineName function) {}

    @Override
    public void checkCanShowCreateFunction(ConnectorSecurityContext context, SchemaRoutineName function) {}

    // Branch operations

    @Override
    public void checkCanShowBranches(ConnectorSecurityContext context, SchemaTableName tableName) {}

    @Override
    public void checkCanCreateBranch(ConnectorSecurityContext context, SchemaTableName tableName, String branchName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "CREATE BRANCH");
    }

    @Override
    public void checkCanDropBranch(ConnectorSecurityContext context, SchemaTableName tableName, String branchName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "DROP BRANCH");
    }

    @Override
    public void checkCanFastForwardBranch(ConnectorSecurityContext context, SchemaTableName tableName, String sourceBranchName, String targetBranchName)
    {
        checkTablePrivilegeWrite(context, tableName, "MODIFY", "FAST FORWARD BRANCH");
    }

    @Override
    public void checkCanGrantTableBranchPrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, String branchName, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanDenyTableBranchPrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, String branchName, TrinoPrincipal grantee)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    @Override
    public void checkCanRevokeTableBranchPrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, String branchName, TrinoPrincipal revokee, boolean grantOption)
    {
        throw new AccessDeniedException("Privilege management must be done through Unity Catalog directly");
    }

    // Row filters and column masks — not supported for Delta Lake

    @Override
    public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        return Optional.empty();
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnSchema> columns)
    {
        return ImmutableMap.of();
    }

    private record PermissionCacheKey(String principal, String securableType, String fullName) {}
}
