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
package io.trino.unity;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

public class UnityCatalogConfig
{
    public enum AuthType
    {
        STATIC,
        EXTRA_CREDENTIALS,
        OAUTH2,
    }

    private URI serverUri;
    private String catalogName;
    private AuthType authType = AuthType.STATIC;
    private String staticToken;
    private String extraCredentialName = "unity-catalog.token";
    private boolean fallbackToStaticToken;
    private boolean credentialVendingEnabled;
    private boolean allowHttpEndpoint;
    private boolean allowLoopbackEndpoint;
    private boolean validateTokenIdentity = true;
    private String tokenIdentityClaim = "email";
    private boolean bypassPermissionCacheOnWrite = true;
    private boolean bypassCredentialCacheOnWrite = true;

    @NotNull
    public URI getServerUri()
    {
        return serverUri;
    }

    @Config("unity-catalog.server-uri")
    @ConfigDescription("Base URI for the Unity Catalog REST API")
    public UnityCatalogConfig setServerUri(URI serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @NotNull
    public String getCatalogName()
    {
        return catalogName;
    }

    @Config("unity-catalog.catalog-name")
    @ConfigDescription("Unity Catalog catalog name to use")
    public UnityCatalogConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    @NotNull
    public AuthType getAuthType()
    {
        return authType;
    }

    @Config("unity-catalog.auth-type")
    @ConfigDescription("Authentication type: STATIC, EXTRA_CREDENTIALS, or OAUTH2")
    public UnityCatalogConfig setAuthType(AuthType authType)
    {
        this.authType = authType;
        return this;
    }

    public String getStaticToken()
    {
        return staticToken;
    }

    @Config("unity-catalog.static-token")
    @ConfigDescription("Static Bearer token for Unity Catalog authentication")
    @ConfigSecuritySensitive
    public UnityCatalogConfig setStaticToken(String staticToken)
    {
        this.staticToken = staticToken;
        return this;
    }

    @NotNull
    public String getExtraCredentialName()
    {
        return extraCredentialName;
    }

    @Config("unity-catalog.extra-credential-name")
    @ConfigDescription("Name of the extra credential key containing the per-user UC token")
    public UnityCatalogConfig setExtraCredentialName(String extraCredentialName)
    {
        this.extraCredentialName = extraCredentialName;
        return this;
    }

    public boolean isFallbackToStaticToken()
    {
        return fallbackToStaticToken;
    }

    @Config("unity-catalog.fallback-to-static-token")
    @ConfigDescription("Whether to fall back to the static token when per-user credential is missing")
    public UnityCatalogConfig setFallbackToStaticToken(boolean fallbackToStaticToken)
    {
        this.fallbackToStaticToken = fallbackToStaticToken;
        return this;
    }

    public boolean isCredentialVendingEnabled()
    {
        return credentialVendingEnabled;
    }

    @Config("unity-catalog.credential-vending-enabled")
    @ConfigDescription("Whether to use Unity Catalog credential vending for storage access")
    public UnityCatalogConfig setCredentialVendingEnabled(boolean credentialVendingEnabled)
    {
        this.credentialVendingEnabled = credentialVendingEnabled;
        return this;
    }

    public boolean isAllowHttpEndpoint()
    {
        return allowHttpEndpoint;
    }

    @Config("unity-catalog.allow-http-endpoint")
    @ConfigDescription("Allow non-HTTPS endpoint URI (unsafe, for testing only)")
    public UnityCatalogConfig setAllowHttpEndpoint(boolean allowHttpEndpoint)
    {
        this.allowHttpEndpoint = allowHttpEndpoint;
        return this;
    }

    @AssertTrue(message = "Unity Catalog endpoint must use HTTPS unless unity-catalog.allow-http-endpoint is enabled")
    public boolean isEndpointSecure()
    {
        if (serverUri == null || allowHttpEndpoint) {
            return true;
        }
        return "https".equalsIgnoreCase(serverUri.getScheme());
    }

    public boolean isAllowLoopbackEndpoint()
    {
        return allowLoopbackEndpoint;
    }

    @Config("unity-catalog.allow-loopback-endpoint")
    @ConfigDescription("Allow loopback/link-local endpoint addresses (unsafe, for testing only)")
    public UnityCatalogConfig setAllowLoopbackEndpoint(boolean allowLoopbackEndpoint)
    {
        this.allowLoopbackEndpoint = allowLoopbackEndpoint;
        return this;
    }

    @AssertTrue(message = "Unity Catalog endpoint must not use loopback or link-local addresses")
    public boolean isEndpointNotLoopbackOrLinkLocal()
    {
        if (serverUri == null || allowLoopbackEndpoint) {
            return true;
        }
        String host = serverUri.getHost();
        if (host == null) {
            return true;
        }
        try {
            InetAddress address = InetAddress.getByName(host);
            return !address.isLoopbackAddress() && !address.isLinkLocalAddress();
        }
        catch (UnknownHostException e) {
            // Allow — DNS resolution may not work at config validation time
            return true;
        }
    }

    @AssertTrue(message = "Static token is required when auth-type is STATIC")
    public boolean isStaticTokenPresentForStaticAuth()
    {
        if (authType != AuthType.STATIC) {
            return true;
        }
        return staticToken != null;
    }

    @AssertTrue(message = "Fallback to static token requires a static token to be configured")
    public boolean isFallbackRequiresStaticToken()
    {
        if (!fallbackToStaticToken) {
            return true;
        }
        return staticToken != null;
    }

    public boolean isValidateTokenIdentity()
    {
        return validateTokenIdentity;
    }

    @Config("unity-catalog.validate-token-identity")
    @ConfigDescription("Whether to validate that the JWT token identity matches the Trino user")
    public UnityCatalogConfig setValidateTokenIdentity(boolean validateTokenIdentity)
    {
        this.validateTokenIdentity = validateTokenIdentity;
        return this;
    }

    @NotNull
    public String getTokenIdentityClaim()
    {
        return tokenIdentityClaim;
    }

    @Config("unity-catalog.token-identity-claim")
    @ConfigDescription("JWT claim to use for token identity validation (default: email)")
    public UnityCatalogConfig setTokenIdentityClaim(String tokenIdentityClaim)
    {
        this.tokenIdentityClaim = tokenIdentityClaim;
        return this;
    }

    public boolean isBypassPermissionCacheOnWrite()
    {
        return bypassPermissionCacheOnWrite;
    }

    @Config("unity-catalog.bypass-permission-cache-on-write")
    @ConfigDescription("Always fetch fresh permissions from UC on write operations (DDL/DML), bypassing the permission cache")
    public UnityCatalogConfig setBypassPermissionCacheOnWrite(boolean bypassPermissionCacheOnWrite)
    {
        this.bypassPermissionCacheOnWrite = bypassPermissionCacheOnWrite;
        return this;
    }

    public boolean isBypassCredentialCacheOnWrite()
    {
        return bypassCredentialCacheOnWrite;
    }

    @Config("unity-catalog.bypass-credential-cache-on-write")
    @ConfigDescription("Always fetch fresh vended credentials from UC on write operations, bypassing the credential cache")
    public UnityCatalogConfig setBypassCredentialCacheOnWrite(boolean bypassCredentialCacheOnWrite)
    {
        this.bypassCredentialCacheOnWrite = bypassCredentialCacheOnWrite;
        return this;
    }

    @AssertTrue(message = "EXTRA_CREDENTIALS auth with fallback to static token is not allowed (credential omission would escalate to service principal)")
    public boolean isExtraCredentialsFallbackNotAllowed()
    {
        if (authType != AuthType.EXTRA_CREDENTIALS) {
            return true;
        }
        return !fallbackToStaticToken;
    }
}
