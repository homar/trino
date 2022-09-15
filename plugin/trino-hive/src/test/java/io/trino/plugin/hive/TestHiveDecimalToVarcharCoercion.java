package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;

public class TestHiveDecimalToVarcharCoercion
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    public void testVarcharToSmallDecimalCoercion()
    {
        String tableName = "test_varchar_to_small_decimal_coercion_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1 varchar, col2 int, col3 int) with ( FORMAT = 'PARQUET', partitioned_by = ARRAY['col3'])");
        assertUpdate("INSERT INTO " + tableName + "(col1, col2, col3) VALUES('100', 1, 1)", 1);
        query("ALTER TABLE " + tableName + " DROP COLUMN col1");
        query("ALTER TABLE " + tableName + " ADD COLUMN col1 DECIMAL(10, 0)");
        assertUpdate("INSERT INTO " + tableName + " (col1, col2, col3) VALUES(DECIMAL '1000', 2, 2)", 1);
        assertQuery("SELECT col1, col2, col3 FROM " + tableName, "VALUES ('100', 1, 1), ('1000', 2, 2)");
    }

    @Test
    public void testVarcharToBigDecimalCoercion()
    {
        String tableName = "test_varchar_to_big_decimal_coercion_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1 varchar, col2 int, col3 int) with ( FORMAT = 'PARQUET', partitioned_by = ARRAY['col3'])");
        assertUpdate("INSERT INTO " + tableName + "(col1, col2, col3) VALUES('100', 1, 1)", 1);
        query("ALTER TABLE " + tableName + " DROP COLUMN col1");
        query("ALTER TABLE " + tableName + " ADD COLUMN col1 DECIMAL(25, 0)");
        assertUpdate("INSERT INTO " + tableName + " (col1, col2, col3) VALUES(DECIMAL '1000', 2, 2)", 1);
        assertQuery("SELECT col1, col2, col3 FROM " + tableName, "VALUES ('100', 1, 1), ('1000', 2, 2)");
    }

    @Test
    public void testCantCoerceToDecimalWithPrecisionSmallerThanBoundedVarcharLength()
    {
        String tableName = "test_bounded_varchar_to_decimal_with_smaller_precision__coercion_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1 varchar(5), col2 int, col3 int) with ( FORMAT = 'PARQUET', partitioned_by = ARRAY['col3'])");
        assertUpdate("INSERT INTO " + tableName + "(col1, col2, col3) VALUES('100', 1, 1)", 1);
        query("ALTER TABLE " + tableName + " DROP COLUMN col1");
        query("ALTER TABLE " + tableName + " ADD COLUMN col1 DECIMAL(3, 0)");
        assertQueryFails("INSERT INTO " + tableName + " (col1, col2, col3) VALUES(DECIMAL '1000', 2, 2)",
                "Cannot cast DECIMAL\\(4, 0\\) '1000' to DECIMAL\\(3, 0\\)");
    }

    @Test
    public void testBoundedVarcharCanBeCoercedToDecimalWithBiggerPrecision()
    {
        String tableName = "test_bounded_varchar_to_decimal_with_bigger_precision_coercion_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1 varchar(5), col2 int, col3 int) with ( FORMAT = 'PARQUET', partitioned_by = ARRAY['col3'])");
        assertUpdate("INSERT INTO " + tableName + "(col1, col2, col3) VALUES('100', 1, 1)", 1);
        query("ALTER TABLE " + tableName + " DROP COLUMN col1");
        query("ALTER TABLE " + tableName + " ADD COLUMN col1 DECIMAL(8, 0)");
        assertUpdate("INSERT INTO " + tableName + " (col1, col2, col3) VALUES(DECIMAL '1000', 2, 2)", 1);
        assertQuery("SELECT col1, col2, col3 FROM " + tableName, "VALUES ('100', 1, 1), ('1000', 2, 2)");
    }
}
