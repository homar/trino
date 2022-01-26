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
package io.trino.plugin.iceberg.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

public class IcebergVacuumHandle
        extends IcebergProcedureHandle
{
    private final Duration retentionThreshold;

    @JsonCreator
    public IcebergVacuumHandle(Duration retentionThreshold)
    {
        this.retentionThreshold = retentionThreshold;
    }

    @JsonProperty
    public Duration getRetentionThreshold()
    {
        return retentionThreshold;
    }
}
