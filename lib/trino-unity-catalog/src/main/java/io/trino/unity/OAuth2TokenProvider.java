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

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

public class OAuth2TokenProvider
        implements UnityCatalogTokenProvider
{
    private static final String OAUTH2_ACCESS_TOKEN_KEY = "internal$oauth2.access-token";

    private final Optional<String> fallbackToken;

    public OAuth2TokenProvider(Optional<String> fallbackToken)
    {
        this.fallbackToken = requireNonNull(fallbackToken, "fallbackToken is null");
    }

    @Override
    public String token(ConnectorIdentity identity)
    {
        String token = identity.getExtraCredentials().get(OAUTH2_ACCESS_TOKEN_KEY);
        if (token != null) {
            return token;
        }
        return fallbackToken.orElseThrow(() -> new TrinoException(
                PERMISSION_DENIED,
                "OAuth2 access token not available for Unity Catalog and no fallback is configured"));
    }
}
