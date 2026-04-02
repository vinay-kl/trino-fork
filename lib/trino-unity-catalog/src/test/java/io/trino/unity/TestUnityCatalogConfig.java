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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

final class TestUnityCatalogConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(UnityCatalogConfig.class)
                .setServerUri(null)
                .setCatalogName(null)
                .setAuthType(UnityCatalogConfig.AuthType.STATIC)
                .setStaticToken(null)
                .setExtraCredentialName("unity-catalog.token")
                .setFallbackToStaticToken(false)
                .setCredentialVendingEnabled(false)
                .setAllowHttpEndpoint(false)
                .setAllowLoopbackEndpoint(false)
                .setValidateTokenIdentity(true)
                .setTokenIdentityClaim("email")
                .setBypassPermissionCacheOnWrite(true)
                .setBypassCredentialCacheOnWrite(true));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("unity-catalog.server-uri", "https://my-workspace.cloud.databricks.com/api/2.1/unity-catalog")
                .put("unity-catalog.catalog-name", "main")
                .put("unity-catalog.auth-type", "OAUTH2")
                .put("unity-catalog.static-token", "dapi1234567890")
                .put("unity-catalog.extra-credential-name", "uc.token")
                .put("unity-catalog.fallback-to-static-token", "true")
                .put("unity-catalog.credential-vending-enabled", "true")
                .put("unity-catalog.allow-http-endpoint", "true")
                .put("unity-catalog.allow-loopback-endpoint", "true")
                .put("unity-catalog.validate-token-identity", "false")
                .put("unity-catalog.token-identity-claim", "sub")
                .put("unity-catalog.bypass-permission-cache-on-write", "false")
                .put("unity-catalog.bypass-credential-cache-on-write", "false")
                .buildOrThrow();

        UnityCatalogConfig expected = new UnityCatalogConfig()
                .setServerUri(URI.create("https://my-workspace.cloud.databricks.com/api/2.1/unity-catalog"))
                .setCatalogName("main")
                .setAuthType(UnityCatalogConfig.AuthType.OAUTH2)
                .setStaticToken("dapi1234567890")
                .setExtraCredentialName("uc.token")
                .setFallbackToStaticToken(true)
                .setCredentialVendingEnabled(true)
                .setAllowHttpEndpoint(true)
                .setAllowLoopbackEndpoint(true)
                .setValidateTokenIdentity(false)
                .setTokenIdentityClaim("sub")
                .setBypassPermissionCacheOnWrite(false)
                .setBypassCredentialCacheOnWrite(false);

        assertFullMapping(properties, expected);
    }

    @Test
    void testHttpsRequired()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("http://localhost:8080"))
                .setAllowHttpEndpoint(false);
        assertThat(config.isEndpointSecure()).isFalse();
    }

    @Test
    void testHttpAllowedWhenExplicitlyEnabled()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("http://localhost:8080"))
                .setAllowHttpEndpoint(true);
        assertThat(config.isEndpointSecure()).isTrue();
    }

    @Test
    void testHttpsAlwaysAccepted()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("https://uc.example.com"));
        assertThat(config.isEndpointSecure()).isTrue();
    }

    @Test
    void testStaticAuthRequiresToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.STATIC)
                .setStaticToken(null);
        assertThat(config.isStaticTokenPresentForStaticAuth()).isFalse();
    }

    @Test
    void testStaticAuthWithToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.STATIC)
                .setStaticToken("my-token");
        assertThat(config.isStaticTokenPresentForStaticAuth()).isTrue();
    }

    @Test
    void testFallbackRequiresStaticToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setFallbackToStaticToken(true)
                .setStaticToken(null);
        assertThat(config.isFallbackRequiresStaticToken()).isFalse();
    }

    @Test
    void testExtraCredentialsAuthPassesStaticTokenValidation()
    {
        // EXTRA_CREDENTIALS auth bypasses the static token requirement — validation passes
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS);
        assertThat(config.isStaticTokenPresentForStaticAuth()).isTrue();
    }

    @Test
    void testOAuth2AuthPassesStaticTokenValidation()
    {
        // OAUTH2 auth bypasses the static token requirement — validation passes
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.OAUTH2);
        assertThat(config.isStaticTokenPresentForStaticAuth()).isTrue();
    }

    @Test
    void testFallbackEnabledWithStaticTokenPassesValidation()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setFallbackToStaticToken(true)
                .setStaticToken("my-token");
        assertThat(config.isFallbackRequiresStaticToken()).isTrue();
    }

    @Test
    void testFallbackDisabledPassesValidationWithoutStaticToken()
    {
        // When fallback is disabled, no static token is needed — validation passes
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setFallbackToStaticToken(false)
                .setStaticToken(null);
        assertThat(config.isFallbackRequiresStaticToken()).isTrue();
    }

    // --- SSRF protection: loopback/link-local ---

    @Test
    void testLoopbackAddressRejected()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("https://127.0.0.1:8080"));
        assertThat(config.isEndpointNotLoopbackOrLinkLocal()).isFalse();
    }

    @Test
    void testLinkLocalAddressRejected()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("https://169.254.169.254"));
        assertThat(config.isEndpointNotLoopbackOrLinkLocal()).isFalse();
    }

    @Test
    void testPublicAddressAllowed()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("https://accounts.cloud.databricks.com"));
        assertThat(config.isEndpointNotLoopbackOrLinkLocal()).isTrue();
    }

    @Test
    void testNullServerUriPassesLoopbackCheck()
    {
        UnityCatalogConfig config = new UnityCatalogConfig();
        assertThat(config.isEndpointNotLoopbackOrLinkLocal()).isTrue();
    }

    @Test
    void testLoopbackRejectedEvenWithAllowHttpEndpoint()
    {
        // allow-http-endpoint waives TLS, but SSRF protection still applies
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setServerUri(URI.create("http://127.0.0.1:8080"))
                .setAllowHttpEndpoint(true);
        assertThat(config.isEndpointNotLoopbackOrLinkLocal()).isFalse();
    }

    @Test
    void testExtraCredentialsWithFallbackNotAllowed()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS)
                .setFallbackToStaticToken(true)
                .setStaticToken("my-token");
        assertThat(config.isExtraCredentialsFallbackNotAllowed()).isFalse();
    }

    @Test
    void testExtraCredentialsWithoutFallbackAllowed()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS);
        assertThat(config.isExtraCredentialsFallbackNotAllowed()).isTrue();
    }

    @Test
    void testOAuth2WithFallbackAllowed()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.OAUTH2)
                .setFallbackToStaticToken(true)
                .setStaticToken("my-token");
        assertThat(config.isExtraCredentialsFallbackNotAllowed()).isTrue();
    }
}
