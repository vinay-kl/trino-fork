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
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTokenIdentityValidator
{
    @Test
    void testNonJwtTokenSkipsValidation()
    {
        // Databricks PAT (no dots) — should pass through without validation
        assertThatCode(() -> TokenIdentityValidator.validateIdentity("dapi1234567890", "alice", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testNonJwtTokenWithOneDotSkipsValidation()
    {
        // Token with one dot — not a JWT (needs exactly 3 parts)
        assertThatCode(() -> TokenIdentityValidator.validateIdentity("some.token", "alice", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidJwtWithMatchingEmailClaim()
    {
        String token = createJwt("{\"email\":\"alice@example.com\",\"sub\":\"uuid-123\"}");
        assertThatCode(() -> TokenIdentityValidator.validateIdentity(token, "alice@example.com", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidJwtWithCaseInsensitiveMatch()
    {
        String token = createJwt("{\"email\":\"Alice@Example.com\"}");
        assertThatCode(() -> TokenIdentityValidator.validateIdentity(token, "alice@example.com", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidJwtWithMismatchedEmailClaim()
    {
        String token = createJwt("{\"email\":\"bob@example.com\"}");
        assertThatThrownBy(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void testFallbackToSubClaim()
    {
        // No email claim, but sub matches
        String token = createJwt("{\"sub\":\"alice\"}");
        assertThatCode(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testFallbackToSubClaimMismatch()
    {
        String token = createJwt("{\"sub\":\"bob\"}");
        assertThatThrownBy(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void testMissingBothClaimsSkipsValidation()
    {
        // Service account tokens may lack both email and sub
        String token = createJwt("{\"aud\":\"some-audience\",\"iss\":\"some-issuer\"}");
        assertThatCode(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .doesNotThrowAnyException();
    }

    @Test
    void testCustomIdentityClaim()
    {
        String token = createJwt("{\"preferred_username\":\"alice\"}");
        assertThatCode(() -> TokenIdentityValidator.validateIdentity(token, "alice", "preferred_username"))
                .doesNotThrowAnyException();
    }

    @Test
    void testMalformedBase64Payload()
    {
        // Valid JWT structure but invalid base64 in payload
        String token = "eyJhbGciOiJSUzI1NiJ9.!!!invalid-base64!!!.signature";
        assertThatThrownBy(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("invalid JWT payload encoding");
    }

    @Test
    void testMalformedJsonPayload()
    {
        // Valid base64 but not valid JSON
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString("not-json".getBytes(UTF_8));
        String token = "eyJhbGciOiJSUzI1NiJ9." + payload + ".signature";
        assertThatThrownBy(() -> TokenIdentityValidator.validateIdentity(token, "alice", "email"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("invalid JWT payload");
    }

    private static String createJwt(String payloadJson)
    {
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString("{\"alg\":\"RS256\"}".getBytes(UTF_8));
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes(UTF_8));
        return header + "." + payload + ".fake-signature";
    }
}
