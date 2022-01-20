package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.spi.connector.TableProcedureMetadata;

import javax.inject.Provider;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.VACUUM;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;

public class VacuumTableProcedure
        implements Provider<TableProcedureMetadata>
{
    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                VACUUM.name(),
                distributedWithFilteringAndRepartitioning(),
                ImmutableList.of(
                        durationProperty(
                                "retention_threshold",
                                "Only remove files older than",
                                Duration.valueOf("7d"),
                                false)));
    }
}
