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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.AllowAllSecurityModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreType;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreTypeConfig;
import io.trino.plugin.deltalake.security.UnityCatalogSecurityModule;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.unity.UnityCatalogConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.lang.String.format;

public class DeltaLakeSecurityModule
        extends AbstractConfigurationAwareModule
{
    public enum DeltaLakeSecurity
    {
        ALLOW_ALL,
        READ_ONLY,
        FILE,
        SYSTEM,
        UNITY_CATALOG,
        /**/
    }

    @Override
    protected void setup(Binder binder)
    {
        DeltaLakeSecurity security = buildConfigObject(DeltaLakeSecurityConfig.class).getSecuritySystem();
        if (security == DeltaLakeSecurity.UNITY_CATALOG) {
            DeltaLakeMetastoreType metastoreType = buildConfigObject(DeltaLakeMetastoreTypeConfig.class).getMetastoreType();
            checkArgument(metastoreType == DeltaLakeMetastoreType.UNITY,
                    "delta.security=UNITY_CATALOG requires hive.metastore=UNITY");
            UnityCatalogConfig unityCatalogConfig = buildConfigObject(UnityCatalogConfig.class);
            checkArgument(unityCatalogConfig.getAuthType() != UnityCatalogConfig.AuthType.STATIC,
                    "delta.security=UNITY_CATALOG is not compatible with unity-catalog.auth-type=STATIC "
                            + "(all users would share the service principal's permissions)");
            checkArgument(!(unityCatalogConfig.getAuthType() == UnityCatalogConfig.AuthType.OAUTH2
                            && unityCatalogConfig.isFallbackToStaticToken()),
                    "delta.security=UNITY_CATALOG is not compatible with OAUTH2 + fallback-to-static-token "
                            + "(users without an OAuth2 token would inherit service principal permissions)");
        }
        install(switch (security) {
            case ALLOW_ALL -> combine(new AllowAllSecurityModule(), usingSystemSecurity(false));
            case READ_ONLY -> combine(new ReadOnlySecurityModule(), usingSystemSecurity(false));
            case FILE -> combine(new FileBasedAccessControlModule(), usingSystemSecurity(false));
            // do not bind a ConnectorAccessControl so the engine will use system security with system roles
            case SYSTEM -> usingSystemSecurity(true);
            case UNITY_CATALOG -> combine(new UnityCatalogSecurityModule(), usingSystemSecurity(false));
        });
    }

    private static Module usingSystemSecurity(boolean system)
    {
        return binder -> binder.bind(boolean.class).annotatedWith(UsingSystemSecurity.class).toInstance(system);
    }
}
