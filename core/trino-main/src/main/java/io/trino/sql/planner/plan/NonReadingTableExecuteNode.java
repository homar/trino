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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;

public class NonReadingTableExecuteNode
        extends PlanNode
{
    private final Symbol output;
    private final TableExecuteHandle executeHandle;
    private final Optional<TableHandle> sourceHandle;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public NonReadingTableExecuteNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("output") Symbol output,
            @JsonProperty("executeHandle") TableExecuteHandle executeHandle,
            @JsonProperty("sourceHandle") Optional<TableHandle> sourceHandle,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        super(id);
        this.output = output;
        this.executeHandle = executeHandle;
        this.sourceHandle = sourceHandle;
        this.schemaTableName = schemaTableName;
    }

    @Override
    @JsonProperty
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(output);
    }

    @JsonProperty
    public Symbol getOutput()
    {
        return output;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new NonReadingTableExecuteNode(getId(), output, executeHandle, sourceHandle, schemaTableName);
    }

    @JsonProperty
    public TableExecuteHandle getExecuteHandle()
    {
        return executeHandle;
    }

    @JsonProperty
    public Optional<TableHandle> getSourceHandle()
    {
        return sourceHandle;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableExecuteCoordinatorOnly(this, context);
    }
}
