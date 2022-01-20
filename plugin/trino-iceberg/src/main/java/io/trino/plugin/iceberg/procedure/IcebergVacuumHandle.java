package io.trino.plugin.iceberg.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import org.apache.iceberg.FileFormat;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class IcebergVacuumHandle
        extends IcebergProcedureHandle
{
    private final String schemaAsJson;
    private final String partitionSpecAsJson;
    private final List<IcebergColumnHandle> tableColumns;
    private final FileFormat fileFormat;
    private final Map<String, String> tableStorageProperties;
    private final Duration retentionThreshold;

    @JsonCreator
    public IcebergVacuumHandle(
            String schemaAsJson,
            String partitionSpecAsJson,
            List<IcebergColumnHandle> tableColumns,
            FileFormat fileFormat,
            Map<String, String> tableStorageProperties,
            Duration retentionThreshold)
    {
        this.schemaAsJson = requireNonNull(schemaAsJson, "schemaAsJson is null");
        this.partitionSpecAsJson = requireNonNull(partitionSpecAsJson, "partitionSpecAsJson is null");
        this.tableColumns = ImmutableList.copyOf(requireNonNull(tableColumns, "tableColumns is null"));
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.tableStorageProperties = ImmutableMap.copyOf(requireNonNull(tableStorageProperties, "tableStorageProperties is null"));
        this.retentionThreshold = requireNonNull(retentionThreshold, "maxScannedFileSize is null");
    }

    @JsonProperty
    public String getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty
    public String getPartitionSpecAsJson()
    {
        return partitionSpecAsJson;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getTableColumns()
    {
        return tableColumns;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<String, String> getTableStorageProperties()
    {
        return tableStorageProperties;
    }

    @JsonProperty
    public Duration getRetentionThreshold()
    {
        return retentionThreshold;
    }
}
