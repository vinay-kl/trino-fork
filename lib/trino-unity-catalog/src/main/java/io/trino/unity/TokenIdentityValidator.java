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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.util.Base64;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

final class TokenIdentityValidator
{
    private static final Logger log = Logger.get(TokenIdentityValidator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TokenIdentityValidator() {}

    static void validateIdentity(String token, String trinoUser, String identityClaim)
    {
        // Check if token looks like a JWT (header.payload.signature)
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            // Non-JWT token (e.g., Databricks PAT "dapi...") — skip validation
            log.debug("Skipping identity validation for non-JWT token");
            return;
        }

        String payload;
        try {
            // JWT payloads use Base64url encoding without padding — add padding if needed
            String base64Payload = parts[1];
            int paddingNeeded = (4 - base64Payload.length() % 4) % 4;
            base64Payload = base64Payload + "=".repeat(paddingNeeded);
            payload = new String(Base64.getUrlDecoder().decode(base64Payload), UTF_8);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(PERMISSION_DENIED, "Unity Catalog token contains invalid JWT payload encoding");
        }

        JsonNode claims;
        try {
            claims = OBJECT_MAPPER.readTree(payload);
        }
        catch (Exception e) {
            throw new TrinoException(PERMISSION_DENIED, "Unity Catalog token contains invalid JWT payload");
        }

        // Try configured claim first, then fall back to "sub"
        String tokenIdentity = null;
        JsonNode claimNode = claims.path(identityClaim);
        if (claimNode.isTextual()) {
            tokenIdentity = claimNode.asText();
        }
        else {
            JsonNode subNode = claims.path("sub");
            if (subNode.isTextual()) {
                tokenIdentity = subNode.asText();
            }
        }

        if (tokenIdentity == null) {
            // Service account tokens may lack both claims — skip validation
            log.debug("Skipping identity validation: JWT has neither '%s' nor 'sub' claim", identityClaim);
            return;
        }

        if (!tokenIdentity.equalsIgnoreCase(trinoUser)) {
            throw new TrinoException(PERMISSION_DENIED,
                    format("Unity Catalog token identity '%s' does not match Trino user '%s'", tokenIdentity, trinoUser));
        }
    }
}
