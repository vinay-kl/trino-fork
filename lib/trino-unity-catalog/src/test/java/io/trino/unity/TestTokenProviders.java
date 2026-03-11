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
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTokenProviders
{
    @Test
    void testStaticTokenProviderReturnsConfiguredToken()
    {
        StaticTokenProvider provider = new StaticTokenProvider("my-token");
        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        assertThat(provider.token(identity)).isEqualTo("my-token");
    }

    @Test
    void testStaticTokenProviderIgnoresIdentity()
    {
        StaticTokenProvider provider = new StaticTokenProvider("same-token");
        ConnectorIdentity alice = ConnectorIdentity.ofUser("alice");
        ConnectorIdentity bob = ConnectorIdentity.ofUser("bob");
        assertThat(provider.token(alice)).isEqualTo(provider.token(bob));
    }

    @Test
    void testExtraCredentialsExtractsToken()
    {
        ExtraCredentialsTokenProvider provider = new ExtraCredentialsTokenProvider("uc.token", Optional.empty());
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice")
                .withExtraCredentials(ImmutableMap.of("uc.token", "alice-token"))
                .build();
        assertThat(provider.token(identity)).isEqualTo("alice-token");
    }

    @Test
    void testExtraCredentialsMissingWithNoFallbackThrows()
    {
        ExtraCredentialsTokenProvider provider = new ExtraCredentialsTokenProvider("uc.token", Optional.empty());
        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        assertThatThrownBy(() -> provider.token(identity))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("uc.token")
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testExtraCredentialsFallsBackToStaticToken()
    {
        ExtraCredentialsTokenProvider provider = new ExtraCredentialsTokenProvider("uc.token", Optional.of("fallback-token"));
        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        assertThat(provider.token(identity)).isEqualTo("fallback-token");
    }

    @Test
    void testExtraCredentialsPrefersUserTokenOverFallback()
    {
        ExtraCredentialsTokenProvider provider = new ExtraCredentialsTokenProvider("uc.token", Optional.of("fallback"));
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice")
                .withExtraCredentials(ImmutableMap.of("uc.token", "alice-token"))
                .build();
        assertThat(provider.token(identity)).isEqualTo("alice-token");
    }

    @Test
    void testExtraCredentialsUsesConfiguredKeyName()
    {
        ExtraCredentialsTokenProvider provider = new ExtraCredentialsTokenProvider("custom.key", Optional.empty());
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice")
                .withExtraCredentials(ImmutableMap.of("custom.key", "custom-token"))
                .build();
        assertThat(provider.token(identity)).isEqualTo("custom-token");
    }

    @Test
    void testOAuth2ExtractsAccessToken()
    {
        OAuth2TokenProvider provider = new OAuth2TokenProvider(Optional.empty());
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice")
                .withExtraCredentials(ImmutableMap.of("internal$oauth2.access-token", "oauth-token"))
                .build();
        assertThat(provider.token(identity)).isEqualTo("oauth-token");
    }

    @Test
    void testOAuth2MissingWithNoFallbackThrows()
    {
        OAuth2TokenProvider provider = new OAuth2TokenProvider(Optional.empty());
        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        assertThatThrownBy(() -> provider.token(identity))
                .isInstanceOf(TrinoException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(PERMISSION_DENIED.toErrorCode());
    }

    @Test
    void testOAuth2FallsBackToStaticToken()
    {
        OAuth2TokenProvider provider = new OAuth2TokenProvider(Optional.of("static-fallback"));
        ConnectorIdentity identity = ConnectorIdentity.ofUser("alice");
        assertThat(provider.token(identity)).isEqualTo("static-fallback");
    }

    @Test
    void testOAuth2PrefersUserTokenOverFallback()
    {
        OAuth2TokenProvider provider = new OAuth2TokenProvider(Optional.of("fallback"));
        ConnectorIdentity identity = ConnectorIdentity.forUser("alice")
                .withExtraCredentials(ImmutableMap.of("internal$oauth2.access-token", "alice-oauth"))
                .build();
        assertThat(provider.token(identity)).isEqualTo("alice-oauth");
    }
}
