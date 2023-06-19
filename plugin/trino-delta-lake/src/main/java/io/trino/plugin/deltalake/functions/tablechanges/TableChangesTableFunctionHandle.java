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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunction.CHANGE_TYPE_COLUMN_NAME;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunction.COMMIT_TIMESTAMP_COLUMN_NAME;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunction.COMMIT_VERSION_COLUMN_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record TableChangesTableFunctionHandle(
        SchemaTableName schemaTableName,
        long firstReadVersion,
        long tableReadVersion,
        String tableLocation,
        List<DeltaLakeColumnHandle> columns,
        TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint)
        implements ConnectorTableFunctionHandle
{
    private static final DeltaLakeColumnHandle CHANGE_TYPE_COLUMN = new DeltaLakeColumnHandle(CHANGE_TYPE_COLUMN_NAME, VARCHAR, OptionalInt.empty(), CHANGE_TYPE_COLUMN_NAME, VARCHAR, SYNTHESIZED, Optional.empty());
    private static final DeltaLakeColumnHandle COMMIT_VERSION_COLUMN = new DeltaLakeColumnHandle(COMMIT_VERSION_COLUMN_NAME, BIGINT, OptionalInt.empty(), COMMIT_VERSION_COLUMN_NAME, BIGINT, SYNTHESIZED, Optional.empty());
    private static final DeltaLakeColumnHandle COMMIT_TIMESTAMP_COLUMN = new DeltaLakeColumnHandle(COMMIT_TIMESTAMP_COLUMN_NAME, TIMESTAMP_TZ_MILLIS, OptionalInt.empty(), COMMIT_TIMESTAMP_COLUMN_NAME, TIMESTAMP_TZ_MILLIS, SYNTHESIZED, Optional.empty());

    public TableChangesTableFunctionHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        requireNonNull(enforcedPartitionConstraint, "enforcedPartitionConstraint is null");
        columns = ImmutableList.copyOf(columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles()
    {
        return Stream.concat(
                        columns.stream(),
                        Stream.of(CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN, COMMIT_TIMESTAMP_COLUMN))
                .collect(Collectors.toMap(DeltaLakeColumnHandle::getColumnName, Function.identity()));
    }

    @Override
    public boolean supportsPredicatePushdown()
    {
        return true;
    }
}
