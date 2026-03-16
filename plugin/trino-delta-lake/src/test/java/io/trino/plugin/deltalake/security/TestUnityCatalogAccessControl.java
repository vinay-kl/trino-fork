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
import io.trino.plugin.deltalake.metastore.unity.TestingUnityCatalogClient;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.unity.UnityCatalogConfig;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogAccessControl
{
    private static final String TOKEN = "test-token";
    private static final String CATALOG = "test_catalog";
    private static final SchemaTableName TABLE = new SchemaTableName("test_schema", "test_table");

    private UnityCatalogAccessControl createAccessControl(TestingUnityCatalogClient client)
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("https://localhost"))
                .setCatalogName(CATALOG)
                .setStaticToken(TOKEN);
        return new UnityCatalogAccessControl(client, identity -> TOKEN, config);
    }

    private ConnectorSecurityContext securityContext()
    {
        return securityContext("alice");
    }

    private ConnectorSecurityContext securityContext(String user)
    {
        return new ConnectorSecurityContext(
                new ConnectorTransactionHandle() {},
                ConnectorIdentity.ofUser(user),
                new QueryId("test_query_id"));
    }

    @Test
    void testSelectRequiresSelectPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
    }

    @Test
    void testSelectDeniedWithoutPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of());
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1")))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("SELECT");
    }

    @Test
    void testInsertRequiresModifyPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanInsertIntoTable(securityContext(), TABLE);
    }

    @Test
    void testInsertDeniedWithOnlySelectPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanInsertIntoTable(securityContext(), TABLE))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("MODIFY");
    }

    @Test
    void testDropTableRequiresManagePrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MANAGE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanDropTable(securityContext(), TABLE);
    }

    @Test
    void testAllPrivilegesGrantsEverything()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("ALL_PRIVILEGES"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
        accessControl.checkCanInsertIntoTable(securityContext(), TABLE);
        accessControl.checkCanDeleteFromTable(securityContext(), TABLE);
        accessControl.checkCanDropTable(securityContext(), TABLE);
        accessControl.checkCanAddColumn(securityContext(), TABLE);
        accessControl.checkCanSetTableProperties(securityContext(), TABLE, ImmutableMap.of());
    }

    @Test
    void testFilterSchemasReturnsAll()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        Set<String> schemas = ImmutableSet.of("schema1", "schema2", "schema3");
        assertThat(accessControl.filterSchemas(securityContext(), schemas)).isEqualTo(schemas);
    }

    @Test
    void testFilterTablesReturnsAll()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        Set<SchemaTableName> tables = ImmutableSet.of(
                new SchemaTableName("s1", "t1"),
                new SchemaTableName("s1", "t2"));
        assertThat(accessControl.filterTables(securityContext(), tables)).isEqualTo(tables);
    }

    @Test
    void testDeleteRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanDeleteFromTable(securityContext(), TABLE);
    }

    @Test
    void testAddColumnRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanAddColumn(securityContext(), TABLE);
        accessControl.checkCanDropColumn(securityContext(), TABLE);
        accessControl.checkCanRenameColumn(securityContext(), TABLE);
    }

    @Test
    void testSetTableCommentRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanSetTableComment(securityContext(), TABLE);
        accessControl.checkCanSetColumnComment(securityContext(), TABLE);
        accessControl.checkCanSetTableProperties(securityContext(), TABLE, ImmutableMap.of());
    }

    @Test
    void testCreateTableRequiresCreateTableOnSchema()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of("CREATE_TABLE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanCreateTable(securityContext(), TABLE, ImmutableMap.of());
    }

    @Test
    void testGrantPrivilegeDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanGrantTablePrivilege(
                securityContext(), null, TABLE, null, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Unity Catalog directly");
    }

    @Test
    void testPrivilegeCaching()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        // First call populates cache
        accessControl.checkCanSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
        assertThat(client.effectivePermissionsCallCount()).isEqualTo(1);

        // Second call should use cache (no additional API calls)
        accessControl.checkCanSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
        assertThat(client.effectivePermissionsCallCount()).isEqualTo(1);
    }

    @Test
    void testCreateSchemaChecksAtCatalogLevel()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("CATALOG", CATALOG, ImmutableList.of("CREATE_SCHEMA"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanCreateSchema(securityContext(), "new_schema", ImmutableMap.of());
    }

    @Test
    void testCreateSchemaDeniedWithoutCatalogPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("CATALOG", CATALOG, ImmutableList.of());
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanCreateSchema(securityContext(), "new_schema", ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("CREATE_SCHEMA");
    }

    @Test
    void testDropSchemaRequiresManage()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of("MANAGE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanDropSchema(securityContext(), "test_schema");
    }

    @Test
    void testRegisterTableProcedureRequiresCreateTable()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of("CREATE_TABLE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanExecuteProcedure(securityContext(),
                new io.trino.spi.connector.SchemaRoutineName("test_schema", "register_table"));
    }

    @Test
    void testRegisterTableProcedureDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of());
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanExecuteProcedure(securityContext(),
                new io.trino.spi.connector.SchemaRoutineName("test_schema", "register_table")))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("CREATE_TABLE");
    }

    @Test
    void testCreateBranchRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanCreateBranch(securityContext(), TABLE, "branch1");
        accessControl.checkCanDropBranch(securityContext(), TABLE, "branch1");
        accessControl.checkCanFastForwardBranch(securityContext(), TABLE, "branch1", "main");
    }

    @Test
    void testBranchPrivilegeManagementDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanGrantTableBranchPrivilege(
                securityContext(), null, TABLE, "branch1", null, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Unity Catalog directly");
    }

    @Test
    void testViewOperationsDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanCreateView(securityContext(), TABLE))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Views are not supported");
        assertThatThrownBy(() -> accessControl.checkCanDropView(securityContext(), TABLE))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Views are not supported");
    }

    @Test
    void testMaterializedViewOperationsDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanCreateMaterializedView(securityContext(), TABLE, ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Materialized views are not supported");
        assertThatThrownBy(() -> accessControl.checkCanDropMaterializedView(securityContext(), TABLE))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Materialized views are not supported");
    }

    @Test
    void testRoleOperationsDenied()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanCreateRole(securityContext(), "admin", java.util.Optional.empty()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanDropRole(securityContext(), "admin"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testTableProcedureRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanExecuteTableProcedure(securityContext(), TABLE, "optimize");
    }

    @Test
    void testTruncateRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanTruncateTable(securityContext(), TABLE);
    }

    @Test
    void testUpdateRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanUpdateTableColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
    }

    @Test
    void testRenameTableRequiresManage()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MANAGE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanRenameTable(securityContext(), TABLE, new SchemaTableName("test_schema", "new_name"));
    }

    @Test
    void testShowCreateTableRequiresSelect()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanShowCreateTable(securityContext(), TABLE);
    }

    @Test
    void testCreateViewWithSelectRequiresSelectPrivilege()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanCreateViewWithSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1"));
    }

    @Test
    void testCreateViewWithSelectDeniedWithoutSelect()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        assertThatThrownBy(() -> accessControl.checkCanCreateViewWithSelectFromColumns(securityContext(), TABLE, ImmutableSet.of("col1")))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("SELECT");
    }

    @Test
    void testNonRegisteredProcedureAllowed()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        // Procedures other than register_table/unregister_table should pass without privilege check
        accessControl.checkCanExecuteProcedure(securityContext(),
                new io.trino.spi.connector.SchemaRoutineName("test_schema", "flush_metadata_cache"));
    }

    @Test
    void testCrossUserCacheIsolation()
    {
        // Verify that cached permissions for user A don't bleed into user B's checks
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("SELECT"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        // Alice queries → succeeds, cache populated
        accessControl.checkCanSelectFromColumns(securityContext("alice"), TABLE, ImmutableSet.of("col1"));
        assertThat(client.effectivePermissionsCallCount()).isEqualTo(1);

        // Remove SELECT privilege in the backend
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of());

        // Alice queries again → still succeeds (served from her cache entry)
        accessControl.checkCanSelectFromColumns(securityContext("alice"), TABLE, ImmutableSet.of("col1"));
        assertThat(client.effectivePermissionsCallCount()).isEqualTo(1);

        // Bob queries → fails (separate cache key triggers fresh API call, gets empty permissions)
        ConnectorSecurityContext bobContext = securityContext("bob");
        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(bobContext, TABLE, ImmutableSet.of("col1")))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(client.effectivePermissionsCallCount()).isEqualTo(2);
    }

    @Test
    void testAlterColumnRequiresModify()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("TABLE", CATALOG + ".test_schema.test_table", ImmutableList.of("MODIFY"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanAlterColumn(securityContext(), TABLE);
    }

    @Test
    void testSetSchemaAuthorizationRequiresManage()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of("MANAGE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanSetSchemaAuthorization(securityContext(), "test_schema",
                new io.trino.spi.security.TrinoPrincipal(io.trino.spi.security.PrincipalType.USER, "bob"));
    }

    @Test
    void testRenameSchemaRequiresManage()
    {
        TestingUnityCatalogClient client = new TestingUnityCatalogClient();
        client.setEffectivePermissions("SCHEMA", CATALOG + ".test_schema", ImmutableList.of("MANAGE"));
        UnityCatalogAccessControl accessControl = createAccessControl(client);

        accessControl.checkCanRenameSchema(securityContext(), "test_schema", "new_schema");
    }
}
