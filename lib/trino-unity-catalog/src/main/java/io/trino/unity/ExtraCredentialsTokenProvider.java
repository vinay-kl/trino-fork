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
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExtraCredentialsTokenProvider
        implements UnityCatalogTokenProvider
{
    private final String credentialName;
    private final Optional<String> fallbackToken;
    private final boolean validateTokenIdentity;
    private final String tokenIdentityClaim;

    public ExtraCredentialsTokenProvider(String credentialName, Optional<String> fallbackToken)
    {
        this(credentialName, fallbackToken, false, "email");
    }

    public ExtraCredentialsTokenProvider(String credentialName, Optional<String> fallbackToken, boolean validateTokenIdentity, String tokenIdentityClaim)
    {
        this.credentialName = requireNonNull(credentialName, "credentialName is null");
        this.fallbackToken = requireNonNull(fallbackToken, "fallbackToken is null");
        this.validateTokenIdentity = validateTokenIdentity;
        this.tokenIdentityClaim = requireNonNull(tokenIdentityClaim, "tokenIdentityClaim is null");
    }

    @Override
    public String token(ConnectorIdentity identity)
    {
        String token = identity.getExtraCredentials().get(credentialName);
        if (token != null) {
            if (validateTokenIdentity) {
                TokenIdentityValidator.validateIdentity(token, identity.getUser(), tokenIdentityClaim);
            }
            return token;
        }
        return fallbackToken.orElseThrow(() -> new TrinoException(
                PERMISSION_DENIED,
                format("Unity Catalog token not found in extra credentials key '%s' and no fallback is configured", credentialName)));
    }
}
