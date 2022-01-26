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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.connector.CatalogName;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * TableExecuteHandle wraps connectors ConnectorTableExecuteHandle which identifies instance of executing
 * specific table procedure o specific table. See {#link {@link ConnectorTableExecuteHandle}} for more details.
 */
public final class TableExecuteHandle
{
    private final CatalogName catalogName;
    private final boolean readsData;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorTableExecuteHandle connectorHandle;

    @JsonCreator
    public TableExecuteHandle(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("readsData") boolean readsData,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("connectorHandle") ConnectorTableExecuteHandle connectorHandle)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.readsData = readsData;
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorTableExecuteHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public boolean isReadsData()
    {
        return readsData;
    }

    public TableExecuteHandle withConnectorHandle(ConnectorTableExecuteHandle connectorHandle, boolean readsData)
    {
        return new TableExecuteHandle(catalogName, readsData, transactionHandle, connectorHandle);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableExecuteHandle that = (TableExecuteHandle) obj;
        return readsData == that.readsData &&
                Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(connectorHandle, that.connectorHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, readsData, transactionHandle, connectorHandle);
    }

    @Override
    public String toString()
    {
        return "Execute[" + catalogName + ":" + connectorHandle + "]";
    }
}
