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

import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUnityCatalogModule
{
    private static final UnityCatalogModule MODULE = new UnityCatalogModule();

    @Test
    void testStaticAuthCreatesStaticTokenProvider()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.STATIC)
                .setStaticToken("my-token");

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(StaticTokenProvider.class);
    }

    @Test
    void testExtraCredentialsAuthCreatesExtraCredentialsProvider()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS)
                .setExtraCredentialName("custom.key");

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(ExtraCredentialsTokenProvider.class);
    }

    @Test
    void testOAuth2AuthCreatesOAuth2Provider()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.OAUTH2);

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(OAuth2TokenProvider.class);
    }

    @Test
    void testFallbackEnabledPassesStaticToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS)
                .setFallbackToStaticToken(true)
                .setStaticToken("fallback-token");

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(ExtraCredentialsTokenProvider.class);
        // Verify fallback works by calling with identity that has no extra credentials
        String token = provider.token(ConnectorIdentity.ofUser("alice"));
        assertThat(token).isEqualTo("fallback-token");
    }

    @Test
    void testFallbackDisabledDoesNotPassStaticToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.EXTRA_CREDENTIALS)
                .setFallbackToStaticToken(false)
                .setStaticToken("should-not-be-used");

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(ExtraCredentialsTokenProvider.class);
        // Without fallback, missing credential should throw — proving static token is NOT passed
        assertThatThrownBy(() -> provider.token(ConnectorIdentity.ofUser("alice")))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    void testOAuth2FallbackEnabledPassesStaticToken()
    {
        UnityCatalogConfig config = new UnityCatalogConfig()
                .setAuthType(UnityCatalogConfig.AuthType.OAUTH2)
                .setFallbackToStaticToken(true)
                .setStaticToken("oauth-fallback");

        UnityCatalogTokenProvider provider = MODULE.tokenProvider(config);

        assertThat(provider).isInstanceOf(OAuth2TokenProvider.class);
        String token = provider.token(ConnectorIdentity.ofUser("alice"));
        assertThat(token).isEqualTo("oauth-fallback");
    }
}
