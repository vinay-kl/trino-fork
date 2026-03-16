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
package io.trino.plugin.deltalake.metastore;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestNoOpVendedCredentialsProvider
{
    @Test
    void testReturnsSameHandle()
    {
        NoOpVendedCredentialsProvider provider = new NoOpVendedCredentialsProvider();
        VendedCredentialsHandle handle = VendedCredentialsHandle.empty("s3://bucket/table");
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result).isSameAs(handle);
    }

    @Test
    void testReturnsSameHandleWithCredentials()
    {
        NoOpVendedCredentialsProvider provider = new NoOpVendedCredentialsProvider();
        VendedCredentialsHandle handle = new VendedCredentialsHandle(
                true, false, "s3://bucket/table", Optional.of("table-id"), VendedCredentialsHandle.READ, Optional.empty());
        ConnectorSession session = TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.ofUser("alice"))
                .build();

        VendedCredentialsHandle result = provider.getFreshCredentials(session, handle);
        assertThat(result).isSameAs(handle);
        assertThat(result.vendedCredentials()).isEmpty();
    }
}
