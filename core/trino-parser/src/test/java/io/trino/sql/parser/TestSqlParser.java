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
package io.trino.sql.parser;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.Descriptor;
import io.trino.sql.tree.DescriptorField;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.EmptyTableTreatment;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionCall.NullTreatment;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantOnType;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IntervalLiteral.IntervalField;
import io.trino.sql.tree.IntervalLiteral.Sign;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonObjectMember;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonTablePlan;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.NestedColumns;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.OrdinalityColumn;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternSearchMode;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.PlanLeaf;
import io.trino.sql.tree.PlanParentChild;
import io.trino.sql.tree.PlanSiblings;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.PrincipalSpecification.Type;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetColumnType;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableFunctionArgument;
import io.trino.sql.tree.TableFunctionInvocation;
import io.trino.sql.tree.TableFunctionTableArgument;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WindowDefinition;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.ascending;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.ordering;
import static io.trino.sql.QueryUtil.query;
import static io.trino.sql.QueryUtil.quotedIdentifier;
import static io.trino.sql.QueryUtil.row;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.subquery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.QueryUtil.values;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParserAssert.assertExpressionIsInvalid;
import static io.trino.sql.parser.ParserAssert.assertStatementIsInvalid;
import static io.trino.sql.parser.ParserAssert.expression;
import static io.trino.sql.parser.ParserAssert.rowPattern;
import static io.trino.sql.parser.ParserAssert.statement;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.REJECT;
import static io.trino.sql.parser.TreeNodes.columnDefinition;
import static io.trino.sql.parser.TreeNodes.dateTimeType;
import static io.trino.sql.parser.TreeNodes.field;
import static io.trino.sql.parser.TreeNodes.location;
import static io.trino.sql.parser.TreeNodes.property;
import static io.trino.sql.parser.TreeNodes.qualifiedName;
import static io.trino.sql.parser.TreeNodes.rowType;
import static io.trino.sql.parser.TreeNodes.simpleType;
import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static io.trino.sql.tree.ArithmeticUnaryExpression.negative;
import static io.trino.sql.tree.ArithmeticUnaryExpression.positive;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.DateTimeDataType.Type.TIMESTAMP;
import static io.trino.sql.tree.EmptyTableTreatment.Treatment.PRUNE;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.FOLLOWING;
import static io.trino.sql.tree.JsonPathParameter.JsonFormat.JSON;
import static io.trino.sql.tree.JsonPathParameter.JsonFormat.UTF16;
import static io.trino.sql.tree.JsonPathParameter.JsonFormat.UTF32;
import static io.trino.sql.tree.JsonPathParameter.JsonFormat.UTF8;
import static io.trino.sql.tree.PatternSearchMode.Mode.SEEK;
import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static io.trino.sql.tree.ProcessingMode.Mode.RUNNING;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.sql.tree.SaveMode.IGNORE;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static io.trino.sql.tree.SetProperties.Type.MATERIALIZED_VIEW;
import static io.trino.sql.tree.SkipTo.skipToNextRow;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.TableFunctionDescriptorArgument.descriptorArgument;
import static io.trino.sql.tree.TableFunctionDescriptorArgument.nullDescriptorArgument;
import static io.trino.sql.tree.Trim.Specification.BOTH;
import static io.trino.sql.tree.Trim.Specification.LEADING;
import static io.trino.sql.tree.Trim.Specification.TRAILING;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSqlParser
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPosition()
    {
        assertThat(expression("position('a' in 'b')"))
                .isEqualTo(new FunctionCall(location(1, 1), QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral(location(1, 17), "b"),
                        new StringLiteral(location(1, 10), "a"))));

        assertThat(expression("position('a' in ('b'))"))
                .isEqualTo(new FunctionCall(location(1, 1), QualifiedName.of("strpos"), ImmutableList.of(
                        new StringLiteral(location(1, 18), "b"),
                        new StringLiteral(location(1, 10), "a"))));
    }

    @Test
    public void testPossibleExponentialBacktracking()
    {
        createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test
    @Timeout(value = 2, unit = SECONDS)
    public void testPotentialUnboundedLookahead()
    {
        createExpression("""
                (
                      1 * -1 +
                      1 * -2 +
                      1 * -3 +
                      1 * -4 +
                      1 * -5 +
                      1 * -6 +
                      1 * -7 +
                      1 * -8 +
                      1 * -9 +
                      1 * -10 +
                      1 * -11 +
                      1 * -12
                )
                """);
    }

    @Test
    public void testQualifiedName()
    {
        assertThat(QualifiedName.of("a", "b", "c", "d").toString())
                .isEqualTo("a.b.c.d");
        assertThat(QualifiedName.of("A", "b", "C", "d").toString())
                .isEqualTo("a.b.c.d");
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("b", "c", "d")));
        assertTrue(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "b", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("a", "c", "d")));
        assertFalse(QualifiedName.of("a", "b", "c", "d").hasSuffix(QualifiedName.of("z", "a", "b", "c", "d")));
        assertThat(QualifiedName.of("a", "b", "c", "d"))
                .isEqualTo(QualifiedName.of("a", "b", "c", "d"));
    }

    @Test
    public void testGenericLiteral()
    {
        assertGenericLiteral("VARCHAR");
        assertGenericLiteral("BIGINT");
        assertGenericLiteral("DOUBLE");
        assertGenericLiteral("BOOLEAN");
        assertGenericLiteral("DATE");
        assertGenericLiteral("foo");
    }

    private static void assertGenericLiteral(String type)
    {
        assertThat(expression(type + " 'abc'"))
                .isEqualTo(new GenericLiteral(new NodeLocation(1, 1), type, "abc"));
    }

    @Test
    public void testBinaryLiteral()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("x' '"))
                .isEqualTo(new BinaryLiteral(location, ""));
        assertThat(expression("x''"))
                .isEqualTo(new BinaryLiteral(location, ""));
        assertThat(expression("X'abcdef1234567890ABCDEF'"))
                .isEqualTo(new BinaryLiteral(location, "abcdef1234567890ABCDEF"));

        // forms such as "X 'a b' " may look like BinaryLiteral
        // but they do not pass the syntax rule for BinaryLiteral
        // but instead conform to TypeConstructor, which generates a GenericLiteral expression
        assertInvalidExpression("X 'a b'", "Spaces are not allowed.*");
        assertInvalidExpression("X'a b c'", "Binary literal must contain an even number of digits.*");
        assertInvalidExpression("X'a z'", "Binary literal can only contain hexadecimal digits.*");
    }

    @Test
    public void testLiterals()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("TIME 'abc'"))
                .isEqualTo(new TimeLiteral(location, "abc"));
        assertThat(expression("TIMESTAMP 'abc'"))
                .isEqualTo(new TimestampLiteral(location, "abc"));
        assertThat(expression("INTERVAL '33' day"))
                .isEqualTo(new IntervalLiteral(location, "33", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertThat(expression("INTERVAL '33' day to second"))
                .isEqualTo(new IntervalLiteral(location, "33", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertThat(expression("CHAR 'abc'"))
                .isEqualTo(new CharLiteral(location, "abc"));
    }

    @Test
    public void testNumbers()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("9223372036854775807"))
                .isEqualTo(new LongLiteral(location, "9223372036854775807"));
        assertInvalidExpression("9223372036854775808", "Invalid numeric literal: 9223372036854775808");

        assertThat(expression("-9223372036854775808"))
                .isEqualTo(new LongLiteral(location, "-9223372036854775808"));
        assertInvalidExpression("-9223372036854775809", "Invalid numeric literal: -9223372036854775809");

        assertThat(expression("1E5"))
                .isEqualTo(new DoubleLiteral(location, "1E5"));
        assertThat(expression("1E-5"))
                .isEqualTo(new DoubleLiteral(location, "1E-5"));
        assertThat(expression(".1E5"))
                .isEqualTo(new DoubleLiteral(location, ".1E5"));
        assertThat(expression(".1E-5"))
                .isEqualTo(new DoubleLiteral(location, ".1E-5"));
        assertThat(expression("1.1E5"))
                .isEqualTo(new DoubleLiteral(location, "1.1E5"));
        assertThat(expression("1.1E-5"))
                .isEqualTo(new DoubleLiteral(location, "1.1E-5"));

        assertThat(expression("-1E5"))
                .isEqualTo(new DoubleLiteral(location, "-1E5"));
        assertThat(expression("-1E-5"))
                .isEqualTo(new DoubleLiteral(location, "-1E-5"));
        assertThat(expression("-.1E5"))
                .isEqualTo(new DoubleLiteral(location, "-.1E5"));
        assertThat(expression("-.1E-5"))
                .isEqualTo(new DoubleLiteral(location, "-.1E-5"));
        assertThat(expression("-1.1E5"))
                .isEqualTo(new DoubleLiteral(location, "-1.1E5"));
        assertThat(expression("-1.1E-5"))
                .isEqualTo(new DoubleLiteral(location, "-1.1E-5"));

        assertThat(expression(".1"))
                .isEqualTo(new DecimalLiteral(location, ".1"));
        assertThat(expression("1.2"))
                .isEqualTo(new DecimalLiteral(location, "1.2"));
        assertThat(expression("-1.2"))
                .isEqualTo(new DecimalLiteral(location, "-1.2"));

        assertThat(expression("123_456_789"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "123_456_789"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(123456789L));

        assertThatThrownBy(() -> SQL_PARSER.createExpression("123_456_789_", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThat(expression("123_456.789_0123"))
                .isEqualTo(new DecimalLiteral(new NodeLocation(1, 1), "123_456.789_0123"));

        assertThatThrownBy(() -> SQL_PARSER.createExpression("123_456.789_0123_", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThatThrownBy(() -> SQL_PARSER.createExpression("_123_456.789_0123", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThatThrownBy(() -> SQL_PARSER.createExpression("123_456_.789_0123", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThatThrownBy(() -> SQL_PARSER.createExpression("123_456._789_0123", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThat(expression("0x123_abc_def"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0x123_abc_def"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(4893429231L));

        assertThat(expression("0X123_ABC_DEF"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0X123_ABC_DEF"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(4893429231L));

        assertThatThrownBy(() -> SQL_PARSER.createExpression("0x123_ABC_DEF_", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThat(expression("0O012_345"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0O012_345"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(5349L));

        assertThat(expression("0o012_345"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0o012_345"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(5349L));

        assertThatThrownBy(() -> SQL_PARSER.createExpression("0o012_345_", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);

        assertThat(expression("0B110_010"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0B110_010"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(50L));

        assertThat(expression("0b110_010"))
                .isEqualTo(new LongLiteral(new NodeLocation(1, 1), "0b110_010"))
                .satisfies(value -> assertThat(((LongLiteral) value).getParsedValue()).isEqualTo(50L));

        assertThatThrownBy(() -> SQL_PARSER.createExpression("0b110_010_", new ParsingOptions()))
                .isInstanceOf(ParsingException.class);
    }

    @Test
    public void testIdentifier()
    {
        assertThat(expression("_123_456"))
                .isEqualTo(new Identifier(new NodeLocation(1, 1), "_123_456", false));

        assertThat(expression("_0x123_ABC_DEF"))
                .isEqualTo(new Identifier(new NodeLocation(1, 1), "_0x123_ABC_DEF", false));

        assertThat(expression("_0o012_345"))
                .isEqualTo(new Identifier(new NodeLocation(1, 1), "_0o012_345", false));

        assertThat(expression("_0b110_010"))
                .isEqualTo(new Identifier(new NodeLocation(1, 1), "_0b110_010", false));
    }

    @Test
    public void testArray()
    {
        assertThat(expression("ARRAY []"))
                .isEqualTo(new Array(location(1, 1), ImmutableList.of()));
        assertThat(expression("ARRAY [1, 2]"))
                .isEqualTo(new Array(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 8), "1"), new LongLiteral(location(1, 11), "2"))));
        assertThat(expression("ARRAY [1e0, 2.5e0]"))
                .isEqualTo(new Array(location(1, 1), ImmutableList.of(new DoubleLiteral(location(1, 8), "1.0"), new DoubleLiteral(location(1, 13), "2.5"))));
        assertThat(expression("ARRAY ['hi']"))
                .isEqualTo(new Array(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "hi"))));
        assertThat(expression("ARRAY ['hi', 'hello']"))
                .isEqualTo(new Array(location(1, 1), ImmutableList.of(new StringLiteral(location(1, 8), "hi"), new StringLiteral(location(1, 14), "hello"))));
    }

    @Test
    public void testArraySubscript()
    {
        assertThat(expression("ARRAY [1, 2][1]"))
                .isEqualTo(new SubscriptExpression(
                        location(1, 1),
                        new Array(location(1, 1), ImmutableList.of(new LongLiteral(location(1, 8), "1"), new LongLiteral(location(1, 11), "2"))),
                        new LongLiteral(location(1, 14), "1")));

        assertExpression("CASE WHEN TRUE THEN ARRAY[1,2] END[1]", new SubscriptExpression(
                new SearchedCaseExpression(
                        ImmutableList.of(
                                new WhenClause(
                                        new BooleanLiteral("true"),
                                        new Array(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))))),
                        Optional.empty()),
                new LongLiteral("1")));
    }

    @Test
    public void testRowSubscript()
    {
        assertThat(expression("ROW (1, 'a', true)[1]"))
                .isEqualTo(new SubscriptExpression(
                        location(1, 1),
                        new Row(
                                location(1, 1),
                                ImmutableList.of(
                                        new LongLiteral(location(1, 6), "1"),
                                        new StringLiteral(location(1, 9), "a"),
                                        new BooleanLiteral(location(1, 14), "true"))),
                        new LongLiteral(location(1, 20), "1")));
    }

    @Test
    public void testAllColumns()
    {
        assertStatement("SELECT * FROM t", simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of()))),
                table(QualifiedName.of("t"))));

        assertStatement("SELECT r.* FROM t", simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.of(new Identifier("r")),
                                        ImmutableList.of()))),
                table(QualifiedName.of("t"))));

        assertStatement("SELECT ROW (1, 'a', true).*", simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new BooleanLiteral("true")))),
                                        ImmutableList.of())))));

        assertStatement("SELECT ROW (1, 'a', true).* AS (f1, f2, f3)", simpleQuery(
                new Select(
                        false,
                        ImmutableList.of(
                                new AllColumns(
                                        Optional.empty(),
                                        Optional.of(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), new BooleanLiteral("true")))),
                                        ImmutableList.of(new Identifier("f1"), new Identifier("f2"), new Identifier("f3")))))));
    }

    @Test
    public void testDouble()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("123E7"))
                .isEqualTo(new DoubleLiteral(location, "123E7"));
        assertThat(expression("123.E7"))
                .isEqualTo(new DoubleLiteral(location, "123E7"));
        assertThat(expression("123.0E7"))
                .isEqualTo(new DoubleLiteral(location, "123E7"));
        assertThat(expression("123E+7"))
                .isEqualTo(new DoubleLiteral(location, "123E7"));
        assertThat(expression("123E-7"))
                .isEqualTo(new DoubleLiteral(location, "123E-7"));

        assertThat(expression("123.456E7"))
                .isEqualTo(new DoubleLiteral(location, "123.456E7"));
        assertThat(expression("123.456E+7"))
                .isEqualTo(new DoubleLiteral(location, "123.456E7"));
        assertThat(expression("123.456E-7"))
                .isEqualTo(new DoubleLiteral(location, "123.456E-7"));

        assertThat(expression(".4E42"))
                .isEqualTo(new DoubleLiteral(location, ".4E42"));
        assertThat(expression(".4E+42"))
                .isEqualTo(new DoubleLiteral(location, ".4E42"));
        assertThat(expression(".4E-42"))
                .isEqualTo(new DoubleLiteral(location, ".4E-42"));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertThat(expression("9"))
                .isEqualTo(new LongLiteral(location(1, 1), "9"));
        assertThat(expression("+9"))
                .isEqualTo(positive(location(1, 1), new LongLiteral(location(1, 2), "9")));
        assertThat(expression("+ 9"))
                .isEqualTo(positive(location(1, 1), new LongLiteral(location(1, 3), "9")));

        assertThat(expression("++9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 2), new LongLiteral(location(1, 3), "9"))));
        assertThat(expression("+ +9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 4), "9"))));
        assertThat(expression("+ + 9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 5), "9"))));

        assertThat(expression("+++9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 2), positive(location(1, 3), new LongLiteral(location(1, 4), "9")))));
        assertThat(expression("+ + +9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 6), "9")))));
        assertThat(expression("+ + + 9"))
                .isEqualTo(positive(location(1, 1), positive(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 7), "9")))));

        assertThat(expression("-9"))
                .isEqualTo(new LongLiteral(location(1, 1), "-9"));
        assertThat(expression("- 9"))
                .isEqualTo(new LongLiteral(location(1, 1), "-9"));

        assertThat(expression("- + 9"))
                .isEqualTo(negative(location(1, 1), positive(location(1, 3), new LongLiteral(location(1, 5), "9"))));
        assertThat(expression("-+9"))
                .isEqualTo(negative(location(1, 1), positive(location(1, 2), new LongLiteral(location(1, 3), "9"))));

        assertThat(expression("+ - + 9"))
                .isEqualTo(positive(location(1, 1), negative(location(1, 3), positive(location(1, 5), new LongLiteral(location(1, 7), "9")))));
        assertThat(expression("+-+9"))
                .isEqualTo(positive(location(1, 1), negative(location(1, 2), positive(location(1, 3), new LongLiteral(location(1, 4), "9")))));

        assertThat(expression("- -9"))
                .isEqualTo(negative(location(1, 1), new LongLiteral(location(1, 3), "-9")));
        assertThat(expression("- - 9"))
                .isEqualTo(negative(location(1, 1), new LongLiteral(location(1, 3), "-9")));

        assertThat(expression("- + - + 9"))
                .isEqualTo(negative(location(1, 1), positive(location(1, 3), negative(location(1, 5), positive(location(1, 7), new LongLiteral(location(1, 9), "9"))))));
        assertThat(expression("-+-+9"))
                .isEqualTo(negative(location(1, 1), positive(location(1, 2), negative(location(1, 3), positive(location(1, 4), new LongLiteral(location(1, 5), "9"))))));

        assertThat(expression("+ - + - + 9"))
                .isEqualTo(positive(location(1, 1), negative(location(1, 3), positive(location(1, 5), negative(location(1, 7), positive(location(1, 9), new LongLiteral(location(1, 11), "9")))))));
        assertThat(expression("+-+-+9"))
                .isEqualTo(positive(location(1, 1), negative(location(1, 2), positive(location(1, 3), negative(location(1, 4), positive(location(1, 5), new LongLiteral(location(1, 6), "9")))))));

        assertThat(expression("- - -9"))
                .isEqualTo(negative(location(1, 1), negative(location(1, 3), new LongLiteral(location(1, 5), "-9"))));
        assertThat(expression("- - - 9"))
                .isEqualTo(negative(location(1, 1), negative(location(1, 3), new LongLiteral(location(1, 5), "-9"))));
    }

    @Test
    public void testCoalesce()
    {
        assertInvalidExpression("coalesce()", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(5)", "The 'coalesce' function must have at least two arguments");
        assertInvalidExpression("coalesce(1, 2) filter (where true)", "FILTER not valid for 'coalesce' function");
        assertInvalidExpression("coalesce(1, 2) OVER ()", "OVER clause not valid for 'coalesce' function");
        assertThat(expression("coalesce(13, 42)"))
                .isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(
                        new LongLiteral(location(1, 10), "13"),
                        new LongLiteral(location(1, 14), "42"))));
        assertThat(expression("coalesce(6, 7, 8)"))
                .isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(
                        new LongLiteral(location(1, 10), "6"),
                        new LongLiteral(location(1, 13), "7"),
                        new LongLiteral(location(1, 16), "8"))));
        assertThat(expression("coalesce(13, null)"))
                .isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(
                        new LongLiteral(location(1, 10), "13"),
                        new NullLiteral(location(1, 14)))));
        assertThat(expression("coalesce(null, 13)"))
                .isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(
                        new NullLiteral(location(1, 10)),
                        new LongLiteral(location(1, 16), "13"))));
        assertThat(expression("coalesce(null, null)"))
                .isEqualTo(new CoalesceExpression(location(1, 1), ImmutableList.of(
                        new NullLiteral(location(1, 10)),
                        new NullLiteral(location(1, 16)))));
    }

    @Test
    public void testIf()
    {
        assertThat(expression("if(true, 1, 0)"))
                .isEqualTo(new IfExpression(
                        location(1, 1),
                        new BooleanLiteral(location(1, 4), "true"),
                        new LongLiteral(location(1, 10), "1"),
                        new LongLiteral(location(1, 13), "0")));
        assertThat(expression("if(true, 3, null)"))
                .isEqualTo(new IfExpression(
                        location(1, 1),
                        new BooleanLiteral(location(1, 4), "true"),
                        new LongLiteral(location(1, 10), "3"),
                        new NullLiteral(location(1, 13))));
        assertThat(expression("if(false, null, 4)"))
                .isEqualTo(new IfExpression(
                        location(1, 1),
                        new BooleanLiteral(location(1, 4), "false"),
                        new NullLiteral(location(1, 11)),
                        new LongLiteral(location(1, 17), "4")));
        assertThat(expression("if(false, null, null)"))
                .isEqualTo(new IfExpression(
                        location(1, 1),
                        new BooleanLiteral(location(1, 4), "false"),
                        new NullLiteral(location(1, 11)),
                        new NullLiteral(location(1, 17))));
        assertThat(expression("if(true, 3)"))
                .isEqualTo(new IfExpression(
                        location(1, 1),
                        new BooleanLiteral(location(1, 4), "true"),
                        new LongLiteral(location(1, 10), "3"),
                        null));
        assertInvalidExpression("IF(true)", "Invalid number of arguments for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) FILTER (WHERE true)", "FILTER not valid for 'if' function");
        assertInvalidExpression("IF(true, 1, 0) OVER()", "OVER clause not valid for 'if' function");
    }

    @Test
    public void testNullIf()
    {
        assertThat(expression("nullif(42, 87)"))
                .isEqualTo(new NullIfExpression(
                        location(1, 1),
                        new LongLiteral(location(1, 8), "42"),
                        new LongLiteral(location(1, 12), "87")));
        assertThat(expression("nullif(42, null)"))
                .isEqualTo(new NullIfExpression(
                        location(1, 1),
                        new LongLiteral(location(1, 8), "42"),
                        new NullLiteral(location(1, 12))));
        assertThat(expression("nullif(null, null)"))
                .isEqualTo(new NullIfExpression(
                        location(1, 1),
                        new NullLiteral(location(1, 8)),
                        new NullLiteral(location(1, 14))));
        assertInvalidExpression("nullif(1)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(1, 2, 3)", "Invalid number of arguments for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) filter (where true)", "FILTER not valid for 'nullif' function");
        assertInvalidExpression("nullif(42, 87) OVER ()", "OVER clause not valid for 'nullif' function");
    }

    @Test
    public void testDoubleInQuery()
    {
        assertStatement("SELECT 123.456E7 FROM DUAL",
                simpleQuery(
                        selectList(new DoubleLiteral("123.456E7")),
                        table(QualifiedName.of("DUAL"))));
    }

    @Test
    public void testIntersect()
    {
        assertStatement("SELECT 123 INTERSECT DISTINCT SELECT 123 INTERSECT ALL SELECT 123",
                query(new Intersect(
                        ImmutableList.of(
                                new Intersect(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()),
                        false)));
    }

    @Test
    public void testUnion()
    {
        assertStatement("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123",
                query(new Union(
                        ImmutableList.of(
                                new Union(ImmutableList.of(createSelect123(), createSelect123()), true),
                                createSelect123()),
                        false)));
    }

    private static QuerySpecification createSelect123()
    {
        return new QuerySpecification(
                selectList(new LongLiteral("123")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testReservedWordIdentifier()
    {
        assertStatement("SELECT id FROM public.orders",
                simpleQuery(
                        selectList(identifier("id")),
                        new Table(QualifiedName.of("public", "orders"))));

        assertStatement("SELECT id FROM \"public\".\"order\"",
                simpleQuery(
                        selectList(identifier("id")),
                        new Table(QualifiedName.of(ImmutableList.of(
                                new Identifier("public", true),
                                new Identifier("order", true))))));

        assertStatement("SELECT id FROM \"public\".\"order\"\"2\"",
                simpleQuery(
                        selectList(identifier("id")),
                        new Table(QualifiedName.of(ImmutableList.of(
                                new Identifier("public", true),
                                new Identifier("order\"2", true))))));
    }

    @Test
    public void testBetween()
    {
        assertThat(expression("1 BETWEEN 2 AND 3"))
                .isEqualTo(new BetweenPredicate(
                        location(1, 3),
                        new LongLiteral(location(1, 1), "1"),
                        new LongLiteral(location(1, 11), "2"),
                        new LongLiteral(location(1, 17), "3")));

        assertThat(expression("1 NOT BETWEEN 2 AND 3"))
                .isEqualTo(new NotExpression(
                        location(1, 3),
                        new BetweenPredicate(
                                location(1, 3),
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 15), "2"),
                                new LongLiteral(location(1, 21), "3"))));
    }

    @Test
    public void testSelectWithLimit()
    {
        assertStatement("SELECT * FROM table1 LIMIT 2",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(new LongLiteral("2")))));

        assertStatement("SELECT * FROM table1 LIMIT ALL",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(new AllRows()))));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) LIMIT ALL",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(new AllRows()))));
    }

    @Test
    public void testValues()
    {
        Query valuesQuery = query(values(
                row(new StringLiteral("a"), new LongLiteral("1"), new DoubleLiteral("2.2")),
                row(new StringLiteral("b"), new LongLiteral("2"), new DoubleLiteral("3.3"))));

        assertStatement("VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0)", valuesQuery);

        assertStatement("SELECT * FROM (VALUES ('a', 1, 2.2e0), ('b', 2, 3.3e0))",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery)));
    }

    @Test
    public void testRowPattern()
    {
        assertThat(rowPattern("(A B)* | CC+? DD?? E | (F | G)"))
                .isEqualTo(
                        new PatternAlternation(
                                location(1, 1),
                                ImmutableList.of(
                                        new PatternAlternation(
                                                location(1, 1),
                                                ImmutableList.of(
                                                        new QuantifiedPattern(
                                                                location(1, 1),
                                                                new PatternConcatenation(
                                                                        location(1, 2),
                                                                        ImmutableList.of(
                                                                                new PatternVariable(location(1, 2), new Identifier(location(1, 2), "A", false)),
                                                                                new PatternVariable(location(1, 4), new Identifier(location(1, 4), "B", false)))),
                                                                new ZeroOrMoreQuantifier(location(1, 6), true)),
                                                        new PatternConcatenation(
                                                                location(1, 10),
                                                                ImmutableList.of(
                                                                        new PatternConcatenation(
                                                                                location(1, 10),
                                                                                ImmutableList.of(
                                                                                        new QuantifiedPattern(location(1, 10), new PatternVariable(location(1, 10), new Identifier(location(1, 10), "CC", false)), new OneOrMoreQuantifier(location(1, 12), false)),
                                                                                        new QuantifiedPattern(location(1, 15), new PatternVariable(location(1, 15), new Identifier(location(1, 15), "DD", false)), new ZeroOrOneQuantifier(location(1, 17), false)))),
                                                                        new PatternVariable(location(1, 20), new Identifier(location(1, 20), "E", false)))))),
                                        new PatternAlternation(
                                                location(1, 25),
                                                ImmutableList.of(
                                                        new PatternVariable(location(1, 25), new Identifier(location(1, 25), "F", false)),
                                                        new PatternVariable(location(1, 29), new Identifier(location(1, 29), "G", false)))))));

        assertThat(rowPattern("A | B | C D E F"))
                .isEqualTo(
                        new PatternAlternation(
                                location(1, 1),
                                ImmutableList.of(
                                        new PatternAlternation(
                                                location(1, 1),
                                                ImmutableList.of(
                                                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                                                        new PatternVariable(location(1, 5), new Identifier(location(1, 5), "B", false)))),
                                        new PatternConcatenation(
                                                location(1, 9),
                                                ImmutableList.of(
                                                        new PatternConcatenation(
                                                                location(1, 9),
                                                                ImmutableList.of(
                                                                        new PatternConcatenation(
                                                                                location(1, 9),
                                                                                ImmutableList.of(
                                                                                        new PatternVariable(location(1, 9), new Identifier(location(1, 9), "C", false)),
                                                                                        new PatternVariable(location(1, 11), new Identifier(location(1, 11), "D", false)))),
                                                                        new PatternVariable(location(1, 13), new Identifier(location(1, 13), "E", false)))),
                                                        new PatternVariable(location(1, 15), new Identifier(location(1, 15), "F", false)))))));

        assertThatThrownBy(() -> SQL_PARSER.createRowPattern("A!"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:2: mismatched input '!'.*");

        assertThatThrownBy(() -> SQL_PARSER.createRowPattern("A**"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:3: mismatched input '*'.*");

        assertThat(rowPattern("A??"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new ZeroOrOneQuantifier(location(1, 2), false)));

        assertThat(rowPattern("^$"))
                .isEqualTo(new PatternConcatenation(
                        location(1, 1),
                        ImmutableList.of(
                                new AnchorPattern(location(1, 1), AnchorPattern.Type.PARTITION_START),
                                new AnchorPattern(location(1, 2), AnchorPattern.Type.PARTITION_END))));

        assertThat(rowPattern("()"))
                .isEqualTo(new EmptyPattern(location(1, 1)));

        assertThat(rowPattern("A{3}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.of(new LongLiteral(location(1, 3), "3")))));

        assertThat(rowPattern("A{3,}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.empty())));

        assertThat(rowPattern("A{,3}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.empty(), Optional.of(new LongLiteral(location(1, 4), "3")))));

        assertThat(rowPattern("A{3,4}"))
                .isEqualTo(new QuantifiedPattern(
                        location(1, 1),
                        new PatternVariable(location(1, 1), new Identifier(location(1, 1), "A", false)),
                        new RangeQuantifier(location(1, 2), true, Optional.of(new LongLiteral(location(1, 3), "3")), Optional.of(new LongLiteral(location(1, 5), "4")))));
    }

    @Test
    public void testPrecedenceAndAssociativity()
    {
        assertThat(expression("1 AND 2 AND 3 AND 4"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.AND,
                        ImmutableList.of(
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 7), "2"),
                                new LongLiteral(location(1, 13), "3"),
                                new LongLiteral(location(1, 19), "4"))));

        assertThat(expression("1 OR 2 OR 3 OR 4"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 6), "2"),
                                new LongLiteral(location(1, 11), "3"),
                                new LongLiteral(location(1, 16), "4"))));

        assertThat(expression("1 AND 2 AND 3 OR 4 AND 5 AND 6 OR 7 AND 8 AND 9"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LogicalExpression(
                                        location(1, 1),
                                        LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral(location(1, 1), "1"),
                                                new LongLiteral(location(1, 7), "2"),
                                                new LongLiteral(location(1, 13), "3"))),
                                new LogicalExpression(
                                        location(1, 18),
                                        LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral(location(1, 18), "4"),
                                                new LongLiteral(location(1, 24), "5"),
                                                new LongLiteral(location(1, 30), "6"))),
                                new LogicalExpression(
                                        location(1, 35),
                                        LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral(location(1, 35), "7"),
                                                new LongLiteral(location(1, 41), "8"),
                                                new LongLiteral(location(1, 47), "9"))))));

        assertThat(expression("1 AND 2 OR 3"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LogicalExpression(
                                        location(1, 1),
                                        LogicalExpression.Operator.AND,
                                        ImmutableList.of(new LongLiteral(location(1, 1), "1"), new LongLiteral(location(1, 7), "2"))),
                                new LongLiteral(location(1, 12), "3"))));

        assertThat(expression("1 OR 2 AND 3"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LongLiteral(location(1, 1), "1"),
                                new LogicalExpression(
                                        location(1, 6),
                                        LogicalExpression.Operator.AND,
                                        ImmutableList.of(new LongLiteral(location(1, 6), "2"), new LongLiteral(location(1, 12), "3"))))));

        assertThat(expression("NOT 1 AND 2"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.AND,
                        ImmutableList.of(
                                new NotExpression(location(1, 1), new LongLiteral(location(1, 5), "1")),
                                new LongLiteral(location(1, 11), "2"))));

        assertThat(expression("NOT 1 OR 2"))
                .isEqualTo(new LogicalExpression(
                        location(1, 1),
                        LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new NotExpression(location(1, 1), new LongLiteral(location(1, 5), "1")),
                                new LongLiteral(location(1, 10), "2"))));

        assertThat(expression("-1 + 2"))
                .isEqualTo(new ArithmeticBinaryExpression(
                        location(1, 4),
                        ArithmeticBinaryExpression.Operator.ADD,
                        new LongLiteral(location(1, 1), "-1"),
                        new LongLiteral(location(1, 6), "2")));

        assertThat(expression("1 - 2 - 3"))
                .isEqualTo(new ArithmeticBinaryExpression(
                        location(1, 7),
                        ArithmeticBinaryExpression.Operator.SUBTRACT,
                        new ArithmeticBinaryExpression(
                                location(1, 3),
                                ArithmeticBinaryExpression.Operator.SUBTRACT,
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 5), "2")),
                        new LongLiteral(location(1, 9), "3")));

        assertThat(expression("1 / 2 / 3"))
                .isEqualTo(new ArithmeticBinaryExpression(
                        location(1, 7),
                        ArithmeticBinaryExpression.Operator.DIVIDE,
                        new ArithmeticBinaryExpression(
                                location(1, 3),
                                ArithmeticBinaryExpression.Operator.DIVIDE,
                                new LongLiteral(location(1, 1), "1"),
                                new LongLiteral(location(1, 5), "2")),
                        new LongLiteral(location(1, 9), "3")));

        assertThat(expression("1 + 2 * 3"))
                .isEqualTo(new ArithmeticBinaryExpression(
                        location(1, 3),
                        ArithmeticBinaryExpression.Operator.ADD,
                        new LongLiteral(location(1, 1), "1"),
                        new ArithmeticBinaryExpression(
                                location(1, 7),
                                ArithmeticBinaryExpression.Operator.MULTIPLY,
                                new LongLiteral(location(1, 5), "2"),
                                new LongLiteral(location(1, 9), "3"))));
    }

    @Test
    public void testInterval()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("INTERVAL '123' YEAR"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.YEAR, Optional.empty()));
        assertThat(expression("INTERVAL '123-3' YEAR TO MONTH"))
                .isEqualTo(new IntervalLiteral(location, "123-3", Sign.POSITIVE, IntervalField.YEAR, Optional.of(IntervalField.MONTH)));
        assertThat(expression("INTERVAL '123' MONTH"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.MONTH, Optional.empty()));
        assertThat(expression("INTERVAL '123' DAY"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.DAY, Optional.empty()));
        assertThat(expression("INTERVAL '123 23:58:53.456' DAY TO SECOND"))
                .isEqualTo(new IntervalLiteral(location, "123 23:58:53.456", Sign.POSITIVE, IntervalField.DAY, Optional.of(IntervalField.SECOND)));
        assertThat(expression("INTERVAL '123' HOUR"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.HOUR, Optional.empty()));
        assertThat(expression("INTERVAL '23:59' HOUR TO MINUTE"))
                .isEqualTo(new IntervalLiteral(location, "23:59", Sign.POSITIVE, IntervalField.HOUR, Optional.of(IntervalField.MINUTE)));
        assertThat(expression("INTERVAL '123' MINUTE"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.MINUTE, Optional.empty()));
        assertThat(expression("INTERVAL '123' SECOND"))
                .isEqualTo(new IntervalLiteral(location, "123", Sign.POSITIVE, IntervalField.SECOND, Optional.empty()));
    }

    @Test
    public void testDecimal()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("DECIMAL '12.34'"))
                .isEqualTo(new DecimalLiteral(location, "12.34"));
        assertThat(expression("DECIMAL '12.'"))
                .isEqualTo(new DecimalLiteral(location, "12."));
        assertThat(expression("DECIMAL '12'"))
                .isEqualTo(new DecimalLiteral(location, "12"));
        assertThat(expression("DECIMAL '.34'"))
                .isEqualTo(new DecimalLiteral(location, ".34"));
        assertThat(expression("DECIMAL '+12.34'"))
                .isEqualTo(new DecimalLiteral(location, "+12.34"));
        assertThat(expression("DECIMAL '+12'"))
                .isEqualTo(new DecimalLiteral(location, "+12"));
        assertThat(expression("DECIMAL '-12.34'"))
                .isEqualTo(new DecimalLiteral(location, "-12.34"));
        assertThat(expression("DECIMAL '-12'"))
                .isEqualTo(new DecimalLiteral(location, "-12"));
        assertThat(expression("DECIMAL '+.34'"))
                .isEqualTo(new DecimalLiteral(location, "+.34"));
        assertThat(expression("DECIMAL '-.34'"))
                .isEqualTo(new DecimalLiteral(location, "-.34"));

        assertThat(expression("123."))
                .isEqualTo(new DecimalLiteral(location, "123."));
        assertThat(expression("123.0"))
                .isEqualTo(new DecimalLiteral(location, "123.0"));
        assertThat(expression(".5"))
                .isEqualTo(new DecimalLiteral(location, ".5"));
        assertThat(expression("123.5"))
                .isEqualTo(new DecimalLiteral(location, "123.5"));

        assertInvalidDecimalExpression("123.", "Unexpected decimal literal: 123.");
        assertInvalidDecimalExpression("123.0", "Unexpected decimal literal: 123.0");
        assertInvalidDecimalExpression(".5", "Unexpected decimal literal: .5");
        assertInvalidDecimalExpression("123.5", "Unexpected decimal literal: 123.5");
    }

    private static void assertInvalidDecimalExpression(String sql, String message)
    {
        assertThatThrownBy(() -> SQL_PARSER.createExpression(sql, new ParsingOptions(REJECT)))
                .isInstanceOfSatisfying(ParsingException.class, e ->
                        assertThat(e.getErrorMessage()).isEqualTo(message));
    }

    @Test
    public void testTime()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("TIME '03:04:05'"))
                .isEqualTo(new TimeLiteral(location, "03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("CURRENT_TIMESTAMP"))
                .isEqualTo(new CurrentTime(location, CurrentTime.Function.TIMESTAMP));
    }

    @Test
    public void testTrim()
    {
        assertThat(expression("trim(BOTH FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), BOTH, new StringLiteral(location(1, 16), " abc "), Optional.empty()));
        assertThat(expression("trim(LEADING FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), LEADING, new StringLiteral(location(1, 19), " abc "), Optional.empty()));
        assertThat(expression("trim(TRAILING FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), TRAILING, new StringLiteral(location(1, 20), " abc "), Optional.empty()));

        assertThat(expression("trim(BOTH ' ' FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), BOTH, new StringLiteral(location(1, 20), " abc "), Optional.of(new StringLiteral(location(1, 11), " "))));
        assertThat(expression("trim(LEADING ' ' FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), LEADING, new StringLiteral(location(1, 23), " abc "), Optional.of(new StringLiteral(location(1, 14), " "))));
        assertThat(expression("trim(TRAILING ' ' FROM ' abc ')"))
                .isEqualTo(new Trim(location(1, 1), TRAILING, new StringLiteral(location(1, 24), " abc "), Optional.of(new StringLiteral(location(1, 15), " "))));

        assertInvalidExpression("trim(FROM ' abc ')", "The 'trim' function must have specification, char or both arguments when it takes FROM");
    }

    @Test
    public void testFormat()
    {
        assertThat(expression("format('%s', 'abc')"))
                .isEqualTo(new Format(
                        location(1, 1),
                        ImmutableList.of(
                                new StringLiteral(location(1, 8), "%s"),
                                new StringLiteral(location(1, 14), "abc"))));

        assertThat(expression("format('%d %s', 123, 'x')"))
                .isEqualTo(new Format(
                        location(1, 1),
                        ImmutableList.of(
                                new StringLiteral(location(1, 8), "%d %s"),
                                new LongLiteral(location(1, 17), "123"),
                                new StringLiteral(location(1, 22), "x"))));
        assertInvalidExpression("format()", "The 'format' function must have at least two arguments");
        assertInvalidExpression("format('%s')", "The 'format' function must have at least two arguments");
    }

    @Test
    public void testCase()
    {
        assertThat(expression("CASE 1 IS NULL WHEN true THEN 2 ELSE 3 END"))
                .isEqualTo(new SimpleCaseExpression(
                        location(1, 1),
                        new IsNullPredicate(location(1, 8), new LongLiteral(location(1, 6), "1")),
                        ImmutableList.of(
                                new WhenClause(
                                        location(1, 16),
                                        new BooleanLiteral(location(1, 21), "true"),
                                        new LongLiteral(location(1, 31), "2"))),
                        Optional.of(new LongLiteral(location(1, 38), "3"))));
    }

    @Test
    public void testSearchedCase()
    {
        assertThat(expression("CASE WHEN a > 3 THEN 23 WHEN b = a THEN 33 END"))
                .isEqualTo(new SearchedCaseExpression(
                        location(1, 1),
                        ImmutableList.of(
                                new WhenClause(
                                        location(1, 6),
                                        new ComparisonExpression(
                                                location(1, 13),
                                                ComparisonExpression.Operator.GREATER_THAN,
                                                new Identifier(location(1, 11), "a", false),
                                                new LongLiteral(location(1, 15), "3")),
                                        new LongLiteral(location(1, 22), "23")),
                                new WhenClause(
                                        location(1, 25),
                                        new ComparisonExpression(
                                                location(1, 32),
                                                ComparisonExpression.Operator.EQUAL,
                                                new Identifier(location(1, 30), "b", false),
                                                new Identifier(location(1, 34), "a", false)),
                                        new LongLiteral(location(1, 41), "33"))),
                        Optional.empty()));
    }

    @Test
    public void testSetSession()
    {
        assertStatement("SET SESSION foo = 'bar'", new SetSession(QualifiedName.of("foo"), new StringLiteral("bar")));
        assertStatement("SET SESSION foo.bar = 'baz'", new SetSession(QualifiedName.of("foo", "bar"), new StringLiteral("baz")));
        assertStatement("SET SESSION foo.bar.boo = 'baz'", new SetSession(QualifiedName.of("foo", "bar", "boo"), new StringLiteral("baz")));

        assertStatement("SET SESSION foo.bar = 'ban' || 'ana'", new SetSession(
                QualifiedName.of("foo", "bar"),
                new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                        new StringLiteral("ban"),
                        new StringLiteral("ana")))));
    }

    @Test
    public void testResetSession()
    {
        assertStatement("RESET SESSION foo.bar", new ResetSession(QualifiedName.of("foo", "bar")));
        assertStatement("RESET SESSION foo", new ResetSession(QualifiedName.of("foo")));
    }

    @Test
    public void testSessionIdentifiers()
    {
        assertStatement("SET SESSION \"foo-bar\".baz = 'x'",
                new SetSession(QualifiedName.of("foo-bar", "baz"), new StringLiteral("x")));
        assertStatementIsInvalid("SET SESSION foo-bar.name = 'value'")
                .withMessage("line 1:16: mismatched input '-'. Expecting: '.', '='");

        assertStatement("RESET SESSION \"foo-bar\".baz",
                new ResetSession(QualifiedName.of("foo-bar", "baz")));
        assertStatementIsInvalid("RESET SESSION foo-bar.name")
                .withMessage("line 1:18: mismatched input '-'. Expecting: '.', <EOF>");
    }

    @Test
    public void testShowSession()
    {
        assertStatement("SHOW SESSION", new ShowSession(Optional.empty(), Optional.empty()));
        assertStatement("SHOW SESSION LIKE '%'", new ShowSession(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW SESSION LIKE '%' ESCAPE '$'", new ShowSession(Optional.of("%"), Optional.of("$")));
    }

    @Test
    public void testShowCatalogs()
    {
        assertStatement("SHOW CATALOGS", new ShowCatalogs(Optional.empty(), Optional.empty()));
        assertStatement("SHOW CATALOGS LIKE '%'", new ShowCatalogs(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'", new ShowCatalogs(Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowSchemas()
    {
        assertStatement("SHOW SCHEMAS", new ShowSchemas(Optional.empty(), Optional.empty(), Optional.empty()));
        assertStatement("SHOW SCHEMAS FROM foo", new ShowSchemas(Optional.of(identifier("foo")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW SCHEMAS IN foo LIKE '%'", new ShowSchemas(Optional.of(identifier("foo")), Optional.of("%"), Optional.empty()));
        assertStatement("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE '$'", new ShowSchemas(Optional.of(identifier("foo")), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowTables()
    {
        assertStatement("SHOW TABLES", new ShowTables(Optional.empty(), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM a", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES FROM \"awesome schema\"", new ShowTables(Optional.of(QualifiedName.of("awesome schema")), Optional.empty(), Optional.empty()));
        assertStatement("SHOW TABLES IN a LIKE '%$_%' ESCAPE '$'", new ShowTables(Optional.of(QualifiedName.of("a")), Optional.of("%$_%"), Optional.of("$")));
    }

    @Test
    public void testShowColumns()
    {
        assertStatement("SHOW COLUMNS FROM a", new ShowColumns(QualifiedName.of("a"), Optional.empty(), Optional.empty()));
        assertStatement("SHOW COLUMNS FROM a.b", new ShowColumns(QualifiedName.of("a", "b"), Optional.empty(), Optional.empty()));
        assertStatement("SHOW COLUMNS FROM \"awesome table\"", new ShowColumns(QualifiedName.of("awesome table"), Optional.empty(), Optional.empty()));
        assertStatement("SHOW COLUMNS FROM \"awesome schema\".\"awesome table\"", new ShowColumns(QualifiedName.of("awesome schema", "awesome table"), Optional.empty(), Optional.empty()));
        assertStatement("SHOW COLUMNS FROM a.b LIKE '%$_%' ESCAPE '$'", new ShowColumns(QualifiedName.of("a", "b"), Optional.of("%$_%"), Optional.of("$")));

        assertStatementIsInvalid("SHOW COLUMNS FROM a.b LIKE null")
                .withMessage("line 1:28: mismatched input 'null'. Expecting: <string>");

        assertStatementIsInvalid("SHOW COLUMNS FROM a.b LIKE 'a' ESCAPE null'")
                .withMessage("line 1:39: mismatched input 'null'. Expecting: <string>");
    }

    @Test
    public void testShowFunctions()
    {
        assertStatement("SHOW FUNCTIONS", new ShowFunctions(Optional.empty(), Optional.empty()));
        assertStatement("SHOW FUNCTIONS LIKE '%'", new ShowFunctions(Optional.of("%"), Optional.empty()));
        assertStatement("SHOW FUNCTIONS LIKE '%' ESCAPE '$'", new ShowFunctions(Optional.of("%"), Optional.of("$")));
    }

    @Test
    public void testSubstringBuiltInFunction()
    {
        String givenString = "ABCDEF";
        assertStatement(format("SELECT substring('%s' FROM 2)", givenString),
                simpleQuery(selectList(
                        new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"))))));

        assertStatement(format("SELECT substring('%s' FROM 2 FOR 3)", givenString),
                simpleQuery(selectList(
                        new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3"))))));
    }

    @Test
    public void testSubstringRegisteredFunction()
    {
        String givenString = "ABCDEF";
        assertStatement(format("SELECT substring('%s', 2)", givenString),
                simpleQuery(selectList(
                        new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"))))));

        assertStatement(format("SELECT substring('%s', 2, 3)", givenString),
                simpleQuery(selectList(
                        new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3"))))));
    }

    @Test
    public void testSelectWithRowType()
    {
        assertStatement("SELECT col1.f1, col2, col3.f1.f2.f3 FROM table1",
                simpleQuery(
                        selectList(
                                new DereferenceExpression(new Identifier("col1"), identifier("f1")),
                                new Identifier("col2"),
                                new DereferenceExpression(
                                        new DereferenceExpression(new DereferenceExpression(new Identifier("col3"), identifier("f1")), identifier("f2")), identifier("f3"))),
                        new Table(QualifiedName.of("table1"))));

        assertStatement("SELECT col1.f1[0], col2, col3[2].f2.f3, col4[4] FROM table1",
                simpleQuery(
                        selectList(
                                new SubscriptExpression(new DereferenceExpression(new Identifier("col1"), identifier("f1")), new LongLiteral("0")),
                                new Identifier("col2"),
                                new DereferenceExpression(new DereferenceExpression(new SubscriptExpression(new Identifier("col3"), new LongLiteral("2")), identifier("f2")), identifier("f3")),
                                new SubscriptExpression(new Identifier("col4"), new LongLiteral("4"))),
                        new Table(QualifiedName.of("table1"))));

        assertStatement("SELECT CAST(ROW(11, 12) AS ROW(COL0 INTEGER, COL1 INTEGER)).col0",
                simpleQuery(
                        selectList(
                                new DereferenceExpression(
                                        new Cast(
                                                new Row(Lists.newArrayList(new LongLiteral("11"), new LongLiteral("12"))),
                                                rowType(location(1, 26),
                                                        field(location(1, 30), "COL0", simpleType(location(1, 35), "INTEGER")),
                                                        field(location(1, 44), "COL1", simpleType(location(1, 49), "INTEGER")))),
                                        identifier("col0")))));
    }

    @Test
    public void testSelectWithOrderBy()
    {
        assertStatement("SELECT * FROM table1 ORDER BY a",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        ordering(ascending("a"))));
    }

    @Test
    public void testSelectWithOffset()
    {
        assertStatement("SELECT * FROM table1 OFFSET 2 ROWS",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new LongLiteral("2"))),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 OFFSET 2",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new LongLiteral("2"))),
                        Optional.empty()));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2 ROWS",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new LongLiteral("2"))),
                        Optional.empty()));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) OFFSET 2",
                simpleQuery(selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new LongLiteral("2"))),
                        Optional.empty()));
    }

    @Test
    public void testSelectWithFetch()
    {
        assertStatement("SELECT * FROM table1 FETCH FIRST 2 ROWS ONLY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(new LongLiteral("2")))));

        assertStatement("SELECT * FROM table1 FETCH NEXT ROW ONLY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(Optional.empty()))));

        Query valuesQuery = query(values(
                row(new LongLiteral("1"), new StringLiteral("1")),
                row(new LongLiteral("2"), new StringLiteral("2"))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) FETCH FIRST ROW ONLY",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(Optional.empty()))));

        assertStatement("SELECT * FROM (VALUES (1, '1'), (2, '2')) FETCH FIRST ROW WITH TIES",
                simpleQuery(
                        selectList(new AllColumns()),
                        subquery(valuesQuery),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(Optional.empty(), true))));

        assertStatement("SELECT * FROM table1 FETCH FIRST 2 ROWS WITH TIES",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(new LongLiteral("2"), true))));

        assertStatement("SELECT * FROM table1 FETCH NEXT ROW WITH TIES",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(Optional.empty(), true))));
    }

    @Test
    public void testSelectWithGroupBy()
    {
        assertStatement("SELECT * FROM table1 GROUP BY a",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of(new Identifier("a")))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY a, b",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(
                                new SimpleGroupBy(ImmutableList.of(new Identifier("a"))),
                                new SimpleGroupBy(ImmutableList.of(new Identifier("b")))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY ()",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(new SimpleGroupBy(ImmutableList.of())))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY GROUPING SETS (a)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(
                                GroupingSets.Type.EXPLICIT,
                                ImmutableList.of(
                                        ImmutableList.of(new Identifier("a"))))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT a, b, GROUPING(a, b) FROM table1 GROUP BY GROUPING SETS ((a), (b))",
                simpleQuery(
                        selectList(
                                DereferenceExpression.from(QualifiedName.of("a")),
                                DereferenceExpression.from(QualifiedName.of("b")),
                                new GroupingOperation(
                                        Optional.empty(),
                                        ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b")))),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(new GroupingSets(
                                GroupingSets.Type.EXPLICIT,
                                ImmutableList.of(
                                        ImmutableList.of(new Identifier("a")),
                                        ImmutableList.of(new Identifier("b"))))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY ALL GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(false, ImmutableList.of(
                                new GroupingSets(
                                        GroupingSets.Type.EXPLICIT,
                                        ImmutableList.of(
                                                ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                                ImmutableList.of(new Identifier("a")),
                                                ImmutableList.of())),
                                new GroupingSets(GroupingSets.Type.CUBE, ImmutableList.of(ImmutableList.of(new Identifier("c")))),
                                new GroupingSets(GroupingSets.Type.ROLLUP, ImmutableList.of(ImmutableList.of(new Identifier("d"))))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("SELECT * FROM table1 GROUP BY DISTINCT GROUPING SETS ((a, b), (a), ()), CUBE (c), ROLLUP (d)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(QualifiedName.of("table1")),
                        Optional.empty(),
                        Optional.of(new GroupBy(true, ImmutableList.of(
                                new GroupingSets(
                                        GroupingSets.Type.EXPLICIT,
                                        ImmutableList.of(
                                                ImmutableList.of(new Identifier("a"), new Identifier("b")),
                                                ImmutableList.of(new Identifier("a")),
                                                ImmutableList.of())),
                                new GroupingSets(GroupingSets.Type.CUBE, ImmutableList.of(ImmutableList.of(new Identifier("c")))),
                                new GroupingSets(GroupingSets.Type.ROLLUP, ImmutableList.of(ImmutableList.of(new Identifier("d"))))))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testCreateCatalog()
    {
        assertStatement("CREATE CATALOG test USING conn",
                new CreateCatalog(new Identifier("test"), false, new Identifier("conn"), ImmutableList.of(), Optional.empty(), Optional.empty()));

        assertStatement("CREATE CATALOG IF NOT EXISTS test USING conn",
                new CreateCatalog(new Identifier("test"), true, new Identifier("conn"), ImmutableList.of(), Optional.empty(), Optional.empty()));

        assertStatement("CREATE CATALOG test USING conn COMMENT 'awesome' AUTHORIZATION ROLE dragon WITH (\"a\" = 'apple', \"b\" = 123)",
                new CreateCatalog(
                        new Identifier("test"),
                        false,
                        new Identifier("conn"),
                        ImmutableList.of(
                                new Property(new Identifier("a"), new StringLiteral("apple")),
                                new Property(new Identifier("b"), new LongLiteral("123"))),
                        Optional.of(new PrincipalSpecification(Type.ROLE, new Identifier("dragon"))),
                        Optional.of("awesome")));

        assertStatement("CREATE CATALOG \"some name that contains space\" USING \"conn-with-dash\"",
                new CreateCatalog(
                        new Identifier("some name that contains space"),
                        false,
                        new Identifier("conn-with-dash"),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testDropCatalog()
    {
        assertStatement("DROP CATALOG test",
                new DropCatalog(new Identifier("test"), false, false));

        assertStatement("DROP CATALOG test CASCADE",
                new DropCatalog(new Identifier("test"), false, true));

        assertStatement("DROP CATALOG IF EXISTS test",
                new DropCatalog(new Identifier("test"), true, false));

        assertStatement("DROP CATALOG IF EXISTS test RESTRICT",
                new DropCatalog(new Identifier("test"), true, false));

        assertStatement("DROP CATALOG \"some catalog that contains space\"",
                new DropCatalog(new Identifier("some catalog that contains space"), false, false));
    }

    @Test
    public void testCreateSchema()
    {
        assertStatement("CREATE SCHEMA test",
                new CreateSchema(QualifiedName.of("test"), false, ImmutableList.of()));

        assertStatement("CREATE SCHEMA IF NOT EXISTS test",
                new CreateSchema(QualifiedName.of("test"), true, ImmutableList.of()));

        assertStatement("CREATE SCHEMA test WITH (a = 'apple', b = 123)",
                new CreateSchema(
                        QualifiedName.of("test"),
                        false,
                        ImmutableList.of(
                                new Property(new Identifier("a"), new StringLiteral("apple")),
                                new Property(new Identifier("b"), new LongLiteral("123")))));

        assertStatement("CREATE SCHEMA \"some name that contains space\"",
                new CreateSchema(QualifiedName.of("some name that contains space"), false, ImmutableList.of()));
    }

    @Test
    public void testDropSchema()
    {
        assertStatement("DROP SCHEMA test",
                new DropSchema(QualifiedName.of("test"), false, false));

        assertStatement("DROP SCHEMA test CASCADE",
                new DropSchema(QualifiedName.of("test"), false, true));

        assertStatement("DROP SCHEMA IF EXISTS test",
                new DropSchema(QualifiedName.of("test"), true, false));

        assertStatement("DROP SCHEMA IF EXISTS test RESTRICT",
                new DropSchema(QualifiedName.of("test"), true, false));

        assertStatement("DROP SCHEMA \"some schema that contains space\"",
                new DropSchema(QualifiedName.of("some schema that contains space"), false, false));
    }

    @Test
    public void testRenameSchema()
    {
        assertStatement("ALTER SCHEMA foo RENAME TO bar",
                new RenameSchema(QualifiedName.of("foo"), identifier("bar")));

        assertStatement("ALTER SCHEMA foo.bar RENAME TO baz",
                new RenameSchema(QualifiedName.of("foo", "bar"), identifier("baz")));

        assertStatement("ALTER SCHEMA \"awesome schema\".\"awesome table\" RENAME TO \"even more awesome table\"",
                new RenameSchema(QualifiedName.of("awesome schema", "awesome table"), quotedIdentifier("even more awesome table")));
    }

    @Test
    public void testUnicodeString()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("U&''"))
                .isEqualTo(new StringLiteral(location, ""));
        assertThat(expression("U&'' UESCAPE ')'"))
                .isEqualTo(new StringLiteral(location, ""));
        assertThat(expression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801'"))
                .isEqualTo(new StringLiteral(location, "hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertThat(expression("U&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertThat(expression("u&'\u6d4B\u8Bd5ABC\\6d4B\\8Bd5'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertThat(expression("u&'\u6d4B\u8Bd5ABC\\\\'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5ABC\\"));
        assertThat(expression("u&'\u6d4B\u8Bd5ABC###8Bd5' UESCAPE '#'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5ABC#\u8Bd5"));
        assertThat(expression("u&'\u6d4B\u8Bd5''A''B''C##''''#8Bd5' UESCAPE '#'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5\'A\'B\'C#\'\'\u8Bd5"));
        assertInvalidExpression("U&  '\u6d4B\u8Bd5ABC\\\\'", ".*mismatched input.*");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\'", "Incomplete escape sequence: ");
        assertInvalidExpression("u&'\u6d4B\u8Bd5ABC\\+'", "Incomplete escape sequence: ");
        assertInvalidExpression("U&'hello\\6dB\\8Bd5'", "Incomplete escape sequence: 6dB.*");
        assertInvalidExpression("U&'hello\\6D4B\\8Bd'", "Incomplete escape sequence: 8Bd");
        assertInvalidExpression("U&'hello\\K6B\\8Bd5'", "Invalid hexadecimal digit: K");
        assertInvalidExpression("U&'hello\\+FFFFFD\\8Bd5'", "Invalid escaped character: FFFFFD");
        assertInvalidExpression("U&'hello\\DBFF'", "Invalid escaped character: DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\+00DBFF'", "Invalid escaped character: 00DBFF\\. Escaped character is a surrogate\\. Use \'\\\\\\+123456\' instead\\.");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '%%'", "Invalid Unicode escape character: %%");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\uDBFF'", "Invalid Unicode escape character: \uDBFF");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '\n'", "Invalid Unicode escape character: \n");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''''", "Invalid Unicode escape character: \'");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ' '", "Invalid Unicode escape character:  ");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE ''", "Empty Unicode escape character");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '1'", "Invalid Unicode escape character: 1");
        assertInvalidExpression("U&'hello\\8Bd5' UESCAPE '+'", "Invalid Unicode escape character: \\+");
        assertThat(expression("U&'hello!6d4B!8Bd5!+10FFFFworld!7F16!7801' UESCAPE '!'"))
                .isEqualTo(new StringLiteral(location, "hello\u6d4B\u8Bd5\uDBFF\uDFFFworld\u7F16\u7801"));
        assertThat(expression("U&'\u6d4B\u8Bd5ABC!6d4B!8Bd5' UESCAPE '!'"))
                .isEqualTo(new StringLiteral(location, "\u6d4B\u8Bd5ABC\u6d4B\u8Bd5"));
        assertExpression("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' UESCAPE '!'",
                new StringLiteral("hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801"));
    }

    @Test
    public void testCreateTable()
    {
        assertThat(statement("CREATE TABLE foo (a VARCHAR, b BIGINT COMMENT 'hello world', c IPADDRESS)"))
                .isEqualTo(new CreateTable(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        ImmutableList.of(
                                columnDefinition(location(1, 19), "a", simpleType(location(1, 21), "VARCHAR")),
                                columnDefinition(location(1, 30), "b", simpleType(location(1, 32), "BIGINT"), true, "hello world"),
                                columnDefinition(location(1, 62), "c", simpleType(location(1, 64), "IPADDRESS"))),
                        FAIL,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c TIMESTAMP)"))
                .isEqualTo(new CreateTable(
                        location(1, 1),
                        qualifiedName(location(1, 28), "bar"),
                        ImmutableList.of(
                                columnDefinition(location(1, 33), "c", dateTimeType(location(1, 35), TIMESTAMP, false), true)),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR WITH (nullable = true, compression = 'LZ4'))"))
                .describedAs("CREATE TABLE with column properties")
                .isEqualTo(
                        new CreateTable(
                                location(1, 1),
                                qualifiedName(location(1, 28), "bar"),
                                ImmutableList.of(
                                        columnDefinition(
                                                location(1, 33),
                                                "c",
                                                simpleType(location(1, 35), "VARCHAR"),
                                                true,
                                                ImmutableList.of(
                                                        property(location(1, 49), "nullable", new BooleanLiteral(location(1, 60), "true")),
                                                        property(location(1, 66), "compression", new StringLiteral(location(1, 80), "LZ4"))))),
                                IGNORE,
                                ImmutableList.of(),
                                Optional.empty()));

        // with LIKE
        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty())),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table, d BIGINT)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.empty()),
                                new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 63), "BIGINT"), true, emptyList(), Optional.empty())),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertStatement("CREATE TABLE IF NOT EXISTS bar (LIKE like_table INCLUDING PROPERTIES)",
                new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.INCLUDING))),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table EXCLUDING PROPERTIES)"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS bar (c VARCHAR, LIKE like_table EXCLUDING PROPERTIES) COMMENT 'test'"))
                .ignoringLocation()
                .isEqualTo(new CreateTable(QualifiedName.of("bar"),
                        ImmutableList.of(
                                new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 35), "VARCHAR"), true, emptyList(), Optional.empty()),
                                new LikeClause(QualifiedName.of("like_table"),
                                        Optional.of(LikeClause.PropertiesOption.EXCLUDING))),
                        IGNORE,
                        ImmutableList.of(),
                        Optional.of("test")));
    }

    @Test
    public void testCreateTableWithNotNull()
    {
        assertThat(statement("""
                CREATE TABLE foo (
                a VARCHAR NOT NULL COMMENT 'column a',
                b BIGINT COMMENT 'hello world',
                c IPADDRESS,
                d INTEGER NOT NULL)
                """))
                .ignoringLocation()
                .isEqualTo(new CreateTable(
                        QualifiedName.of("foo"),
                        ImmutableList.of(
                                new ColumnDefinition(QualifiedName.of("a"), simpleType(location(1, 20), "VARCHAR"), false, emptyList(), Optional.of("column a")),
                                new ColumnDefinition(QualifiedName.of("b"), simpleType(location(1, 59), "BIGINT"), true, emptyList(), Optional.of("hello world")),
                                new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 91), "IPADDRESS"), true, emptyList(), Optional.empty()),
                                new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 104), "INTEGER"), false, emptyList(), Optional.empty())),
                        FAIL,
                        ImmutableList.of(),
                        Optional.empty()));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertThat(statement("CREATE TABLE foo AS SELECT * FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 21),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 21),
                                new Select(location(1, 21), false, ImmutableList.of(new AllColumns(location(1, 28), Optional.empty(), ImmutableList.of()))),
                                Optional.of(new Table(location(1, 35), qualifiedName(location(1, 35), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        true,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE foo(x) AS SELECT a FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 24),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 24),
                                new Select(location(1, 24), false, ImmutableList.of(new SingleColumn(location(1, 31), new Identifier(location(1, 31), "a", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 38), qualifiedName(location(1, 38), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false))),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 26),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 26),
                                new Select(location(1, 26), false, ImmutableList.of(
                                        new SingleColumn(location(1, 33), new Identifier(location(1, 33), "a", false), Optional.empty()),
                                        new SingleColumn(location(1, 35), new Identifier(location(1, 35), "b", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 42), qualifiedName(location(1, 42), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(
                                new Identifier(location(1, 18), "x", false),
                                new Identifier(location(1, 20), "y", false))),
                        Optional.empty()));

        assertThat(statement("CREATE OR REPLACE TABLE foo AS SELECT * FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 25), "foo"), new Query(
                        location(1, 32),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 32),
                                new Select(location(1, 32), false, ImmutableList.of(new AllColumns(location(1, 39), Optional.empty(), ImmutableList.of()))),
                                Optional.of(new Table(location(1, 46), qualifiedName(location(1, 46), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        REPLACE,
                        ImmutableList.of(),
                        true,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("CREATE OR REPLACE TABLE foo(x) AS SELECT a FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 25), "foo"), new Query(
                        location(1, 35),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 35),
                                new Select(location(1, 35), false, ImmutableList.of(new SingleColumn(location(1, 42), new Identifier(location(1, 42), "a", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 49), qualifiedName(location(1, 49), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        REPLACE,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 29), "x", false))),
                        Optional.empty()));

        assertThat(statement("CREATE OR REPLACE TABLE foo(x,y) AS SELECT a,b FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 25), "foo"), new Query(
                        location(1, 37),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 37),
                                new Select(location(1, 37), false, ImmutableList.of(
                                        new SingleColumn(location(1, 44), new Identifier(location(1, 44), "a", false), Optional.empty()),
                                        new SingleColumn(location(1, 46), new Identifier(location(1, 46), "b", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 53), qualifiedName(location(1, 53), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        REPLACE,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(
                                new Identifier(location(1, 29), "x", false),
                                new Identifier(location(1, 31), "y", false))),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 28), "foo"), new Query(
                        location(1, 35),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 35),
                                new Select(location(1, 35), false, ImmutableList.of(new AllColumns(location(1, 42), Optional.empty(), ImmutableList.of()))),
                                Optional.of(new Table(location(1, 49), qualifiedName(location(1, 49), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        IGNORE,
                        ImmutableList.of(),
                        true,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS foo(x) AS SELECT a FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 28), "foo"), new Query(
                        location(1, 38),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 38),
                                new Select(location(1, 38), false, ImmutableList.of(new SingleColumn(location(1, 45), new Identifier(location(1, 45), "a", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 52), qualifiedName(location(1, 52), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        IGNORE,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 32), "x", false))),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE IF NOT EXISTS foo(x,y) AS SELECT a,b FROM t"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 28), "foo"), new Query(
                        location(1, 40),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 40),
                                new Select(location(1, 40), false, ImmutableList.of(
                                        new SingleColumn(location(1, 47), new Identifier(location(1, 47), "a", false), Optional.empty()),
                                        new SingleColumn(location(1, 49), new Identifier(location(1, 49), "b", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 56), qualifiedName(location(1, 56), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        IGNORE,
                        ImmutableList.of(),
                        true,
                        Optional.of(ImmutableList.of(
                                new Identifier(location(1, 32), "x", false),
                                new Identifier(location(1, 34), "y", false))),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE foo AS SELECT * FROM t WITH NO DATA"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 21),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 21),
                                new Select(location(1, 21), false, ImmutableList.of(new AllColumns(location(1, 28), Optional.empty(), ImmutableList.of()))),
                                Optional.of(new Table(location(1, 35), qualifiedName(location(1, 35), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE foo(x) AS SELECT a FROM t WITH NO DATA"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 24),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 24),
                                new Select(location(1, 24), false, ImmutableList.of(new SingleColumn(location(1, 31), new Identifier(location(1, 31), "a", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 38), qualifiedName(location(1, 38), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false))),
                        Optional.empty()));

        assertThat(statement("CREATE TABLE foo(x,y) AS SELECT a,b FROM t WITH NO DATA"))
                .isEqualTo(new CreateTableAsSelect(location(1, 1), qualifiedName(location(1, 14), "foo"), new Query(
                        location(1, 26),
                        Optional.empty(),
                        new QuerySpecification(
                                location(1, 26),
                                new Select(location(1, 26), false, ImmutableList.of(
                                        new SingleColumn(location(1, 33), new Identifier(location(1, 33), "a", false), Optional.empty()),
                                        new SingleColumn(location(1, 35), new Identifier(location(1, 35), "b", false), Optional.empty()))),
                                Optional.of(new Table(location(1, 42), qualifiedName(location(1, 42), "t"))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                        FAIL,
                        ImmutableList.of(),
                        false,
                        Optional.of(ImmutableList.of(
                                new Identifier(location(1, 18), "x", false),
                                new Identifier(location(1, 20), "y", false))),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT * FROM t
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(
                                                location(4, 1),
                                                false,
                                                ImmutableList.of(new AllColumns(location(4, 8), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        true,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo(x)
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a FROM t
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        true,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false))),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo(x,y)
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a,b FROM t
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()), new SingleColumn(location(4, 10), new Identifier(location(4, 10), "b", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 17), qualifiedName(location(4, 17), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        true,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false), new Identifier(location(1, 20), "y", false))),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT * FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(
                                                location(4, 1),
                                                false,
                                                ImmutableList.of(new AllColumns(location(4, 8), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo(x)
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false))),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo(x,y)
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a,b FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()), new SingleColumn(location(4, 10), new Identifier(location(4, 10), "b", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 17), qualifiedName(location(4, 17), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false), new Identifier(location(1, 20), "y", false))),
                        Optional.empty()));

        assertThat(statement("""
                CREATE TABLE foo COMMENT 'test'
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT * FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(
                                                location(4, 1),
                                                false,
                                                ImmutableList.of(new AllColumns(location(4, 8), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.empty(),
                        Optional.of("test")));

        assertThat(statement("""
                CREATE TABLE foo(x) COMMENT 'test'
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 15), qualifiedName(location(4, 15), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false))),
                        Optional.of("test")));

        assertThat(statement("""
                CREATE TABLE foo(x,y) COMMENT 'test'
                WITH ( string = 'bar', long = 42, computed = 'ban' || 'ana', a  = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a,b FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()), new SingleColumn(location(4, 10), new Identifier(location(4, 10), "b", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 17), qualifiedName(location(4, 17), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", false), new StringLiteral(location(2, 17), "bar")),
                                new Property(location(2, 24), new Identifier(location(2, 24), "long", false), new LongLiteral(location(2, 31), "42")),
                                new Property(
                                        location(2, 35),
                                        new Identifier(location(2, 35), "computed", false),
                                        new FunctionCall(location(2, 52), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 46), "ban"), new StringLiteral(location(2, 55), "ana")))),
                                new Property(location(2, 62), new Identifier(location(2, 62), "a", false), new Array(location(2, 67), ImmutableList.of(new StringLiteral(location(2, 74), "v1"), new StringLiteral(location(2, 80), "v2"))))),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false), new Identifier(location(1, 20), "y", false))),
                        Optional.of("test")));

        assertThat(statement("""
                CREATE TABLE foo(x,y) COMMENT 'test'
                WITH ( "string" = 'bar', "long" = 42, computed = 'ban' || 'ana', a = ARRAY[ 'v1', 'v2' ] )
                AS
                SELECT a,b FROM t
                WITH NO DATA
                """))
                .isEqualTo(new CreateTableAsSelect(
                        location(1, 1),
                        qualifiedName(location(1, 14), "foo"),
                        new Query(
                                location(4, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(4, 1),
                                        new Select(location(4, 1), false, ImmutableList.of(new SingleColumn(location(4, 8), new Identifier(location(4, 8), "a", false), Optional.empty()), new SingleColumn(location(4, 10), new Identifier(location(4, 10), "b", false), Optional.empty()))),
                                        Optional.of(new Table(location(4, 17), qualifiedName(location(4, 17), "t"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        FAIL,
                        ImmutableList.of(
                                new Property(location(2, 8), new Identifier(location(2, 8), "string", true), new StringLiteral(location(2, 19), "bar")),
                                new Property(location(2, 26), new Identifier(location(2, 26), "long", true), new LongLiteral(location(2, 35), "42")),
                                new Property(
                                        location(2, 39),
                                        new Identifier(location(2, 39), "computed", false),
                                        new FunctionCall(location(2, 56), QualifiedName.of("concat"), ImmutableList.of(new StringLiteral(location(2, 50), "ban"), new StringLiteral(location(2, 59), "ana")))),
                                new Property(location(2, 66), new Identifier(location(2, 66), "a", false), new Array(location(2, 70), ImmutableList.of(new StringLiteral(location(2, 77), "v1"), new StringLiteral(location(2, 83), "v2"))))),
                        false,
                        Optional.of(ImmutableList.of(new Identifier(location(1, 18), "x", false), new Identifier(location(1, 20), "y", false))),
                        Optional.of("test")));
    }

    @Test
    public void testCreateTableAsWith()
    {
        String queryParenthesizedWith = """
                CREATE TABLE foo
                AS
                ( WITH t(x) AS (VALUES 1)
                TABLE t )
                WITH NO DATA
                """;
        String queryUnparenthesizedWith = """
                CREATE TABLE foo
                AS
                WITH t(x) AS (VALUES 1)
                TABLE t
                WITH NO DATA
                """;
        String queryParenthesizedWithHasAlias = """
                CREATE TABLE foo(a)
                AS
                ( WITH t(x) AS (VALUES 1)
                TABLE t )
                WITH NO DATA
                """;
        String queryUnparenthesizedWithHasAlias = """
                CREATE TABLE foo(a)
                AS
                WITH t(x) AS (VALUES 1)
                TABLE t
                WITH NO DATA
                """;

        QualifiedName table = QualifiedName.of("foo");

        Query query = new Query(
                Optional.of(new With(false, ImmutableList.of(
                        new WithQuery(
                                identifier("t"),
                                query(new Values(ImmutableList.of(new LongLiteral("1")))),
                                Optional.of(ImmutableList.of(identifier("x"))))))),
                new Table(QualifiedName.of("t")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertStatement(queryParenthesizedWith, new CreateTableAsSelect(table, query, FAIL, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertStatement(queryUnparenthesizedWith, new CreateTableAsSelect(table, query, FAIL, ImmutableList.of(), false, Optional.empty(), Optional.empty()));
        assertStatement(queryParenthesizedWithHasAlias, new CreateTableAsSelect(table, query, FAIL, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
        assertStatement(queryUnparenthesizedWithHasAlias, new CreateTableAsSelect(table, query, FAIL, ImmutableList.of(), false, Optional.of(ImmutableList.of(new Identifier("a"))), Optional.empty()));
    }

    @Test
    public void testDropTable()
    {
        assertStatement("DROP TABLE a", new DropTable(QualifiedName.of("a"), false));
        assertStatement("DROP TABLE a.b", new DropTable(QualifiedName.of("a", "b"), false));
        assertStatement("DROP TABLE a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), false));
        assertStatement("DROP TABLE a.\"b/y\".c", new DropTable(QualifiedName.of("a", "b/y", "c"), false));

        assertStatement("DROP TABLE IF EXISTS a", new DropTable(QualifiedName.of("a"), true));
        assertStatement("DROP TABLE IF EXISTS a.b", new DropTable(QualifiedName.of("a", "b"), true));
        assertStatement("DROP TABLE IF EXISTS a.b.c", new DropTable(QualifiedName.of("a", "b", "c"), true));
        assertStatement("DROP TABLE IF EXISTS a.\"b/y\".c", new DropTable(QualifiedName.of("a", "b/y", "c"), true));
    }

    @Test
    public void testTruncateTable()
            throws Exception
    {
        assertStatement("TRUNCATE TABLE a", new TruncateTable(QualifiedName.of("a")));
        assertStatement("TRUNCATE TABLE a.b", new TruncateTable(QualifiedName.of("a", "b")));
        assertStatement("TRUNCATE TABLE a.b.c", new TruncateTable(QualifiedName.of("a", "b", "c")));
    }

    @Test
    public void testDropView()
    {
        assertStatement("DROP VIEW a", new DropView(QualifiedName.of("a"), false));
        assertStatement("DROP VIEW a.b", new DropView(QualifiedName.of("a", "b"), false));
        assertStatement("DROP VIEW a.b.c", new DropView(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP VIEW IF EXISTS a", new DropView(QualifiedName.of("a"), true));
        assertStatement("DROP VIEW IF EXISTS a.b", new DropView(QualifiedName.of("a", "b"), true));
        assertStatement("DROP VIEW IF EXISTS a.b.c", new DropView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testInsertInto()
    {
        Table table = new Table(QualifiedName.of("a", "b/c", "d"));
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertStatement("INSERT INTO a.\"b/c\".d SELECT * FROM t",
                new Insert(table, Optional.empty(), query));

        assertStatement("INSERT INTO a.\"b/c\".d (c1, c2) SELECT * FROM t",
                new Insert(table, Optional.of(ImmutableList.of(identifier("c1"), identifier("c2"))), query));
    }

    @Test
    public void testDelete()
    {
        assertStatement("DELETE FROM t", new Delete(table(QualifiedName.of("t")), Optional.empty()));
        assertStatement("DELETE FROM \"awesome table\"", new Delete(table(QualifiedName.of("awesome table")), Optional.empty()));

        assertStatement("DELETE FROM t WHERE a = b", new Delete(table(QualifiedName.of("t")), Optional.of(
                new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        new Identifier("a"),
                        new Identifier("b")))));
    }

    @Test
    public void testMerge()
    {
        NodeLocation location = new NodeLocation(1, 1);
        assertStatement("""
                        MERGE INTO inventory AS i
                          USING changes AS c
                          ON i.part = c.part
                        WHEN MATCHED AND c.action = 'mod'
                          THEN UPDATE SET
                            qty = qty + c.qty
                          , ts = CURRENT_TIMESTAMP
                        WHEN MATCHED AND c.action = 'del'
                          THEN DELETE
                        WHEN NOT MATCHED AND c.action = 'new'
                          THEN INSERT (part, qty) VALUES (c.part, c.qty)""",
                new Merge(
                        location,
                        new AliasedRelation(location, table(QualifiedName.of("inventory")), new Identifier("i"), null),
                        aliased(table(QualifiedName.of("changes")), "c"),
                        equal(nameReference("i", "part"), nameReference("c", "part")),
                        ImmutableList.of(
                                new MergeUpdate(
                                        Optional.of(equal(nameReference("c", "action"), new StringLiteral("mod"))),
                                        ImmutableList.of(
                                                new MergeUpdate.Assignment(new Identifier("qty"), new ArithmeticBinaryExpression(
                                                        ArithmeticBinaryExpression.Operator.ADD,
                                                        nameReference("qty"),
                                                        nameReference("c", "qty"))),
                                                new MergeUpdate.Assignment(new Identifier("ts"), new CurrentTime(CurrentTime.Function.TIMESTAMP)))),
                                new MergeDelete(
                                        Optional.of(equal(nameReference("c", "action"), new StringLiteral("del")))),
                                new MergeInsert(
                                        Optional.of(equal(nameReference("c", "action"), new StringLiteral("new"))),
                                        ImmutableList.of(new Identifier("part"), new Identifier("qty")),
                                        ImmutableList.of(nameReference("c", "part"), nameReference("c", "qty"))))));
    }

    @Test
    public void testRenameTable()
    {
        assertStatement("ALTER TABLE a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), false));
        assertStatement("ALTER TABLE IF EXISTS a RENAME TO b", new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"), true));
    }

    @Test
    public void testSetTableProperties()
    {
        assertStatement("ALTER TABLE a SET PROPERTIES foo='bar'", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new StringLiteral("bar")))));
        assertStatement("ALTER TABLE a SET PROPERTIES foo=true", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new BooleanLiteral("true")))));
        assertStatement("ALTER TABLE a SET PROPERTIES foo=123", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123")))));
        assertStatement("ALTER TABLE a SET PROPERTIES foo=123, bar=456", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123")), new Property(new Identifier("bar"), new LongLiteral("456")))));
        assertStatement("ALTER TABLE a SET PROPERTIES \" s p a c e \"='bar'", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier(" s p a c e "), new StringLiteral("bar")))));
        assertStatement("ALTER TABLE a SET PROPERTIES foo=123, bar=DEFAULT", new SetProperties(SetProperties.Type.TABLE, QualifiedName.of("a"), ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123")), new Property(new Identifier("bar")))));

        assertStatementIsInvalid("ALTER TABLE a SET PROPERTIES")
                .withMessage("line 1:29: mismatched input '<EOF>'. Expecting: <identifier>");
        assertStatementIsInvalid("ALTER TABLE a SET PROPERTIES ()")
                .withMessage("line 1:30: mismatched input '('. Expecting: <identifier>");
        assertStatementIsInvalid("ALTER TABLE a SET PROPERTIES (foo='bar')")
                .withMessage("line 1:30: mismatched input '('. Expecting: <identifier>");
    }

    @Test
    public void testCommentTable()
    {
        assertStatement("COMMENT ON TABLE a IS 'test'", new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("test")));
        assertStatement("COMMENT ON TABLE a IS ''", new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.of("")));
        assertStatement("COMMENT ON TABLE a IS NULL", new Comment(Comment.Type.TABLE, QualifiedName.of("a"), Optional.empty()));
    }

    @Test
    public void testCommentView()
    {
        assertStatement("COMMENT ON VIEW a IS 'test'", new Comment(Comment.Type.VIEW, QualifiedName.of("a"), Optional.of("test")));
        assertStatement("COMMENT ON VIEW a IS ''", new Comment(Comment.Type.VIEW, QualifiedName.of("a"), Optional.of("")));
        assertStatement("COMMENT ON VIEW a IS NULL", new Comment(Comment.Type.VIEW, QualifiedName.of("a"), Optional.empty()));
    }

    @Test
    public void testCommentColumn()
    {
        assertStatement("COMMENT ON COLUMN a.b IS 'test'", new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.of("test")));
        assertStatement("COMMENT ON COLUMN a.b IS ''", new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.of("")));
        assertStatement("COMMENT ON COLUMN a.b IS NULL", new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b"), Optional.empty()));

        assertStatement("COMMENT ON COLUMN a IS 'test'", new Comment(Comment.Type.COLUMN, QualifiedName.of("a"), Optional.of("test")));
        assertStatement("COMMENT ON COLUMN a.b.c IS 'test'", new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b", "c"), Optional.of("test")));
        assertStatement("COMMENT ON COLUMN a.b.c.d IS 'test'", new Comment(Comment.Type.COLUMN, QualifiedName.of("a", "b", "c", "d"), Optional.of("test")));
    }

    @Test
    public void testRenameColumn()
    {
        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN a TO b"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 33), "a", false))),
                        new Identifier(location(1, 38), "b", false),
                        false,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN a TO b"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 23), "foo", false),
                                new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "a", false))),
                        new Identifier(location(1, 48), "b", false),
                        true,
                        false));

        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN IF EXISTS a TO b"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "a", false))),
                        new Identifier(location(1, 48), "b", false),
                        false,
                        true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN IF EXISTS a TO b"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 23), "foo", false),
                                new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 53), "a", false))),
                        new Identifier(location(1, 58), "b", false),
                        true,
                        true));

        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN c.d TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 33), "c", false), new Identifier(location(1, 35), "d", false))),
                        new Identifier(location(1, 40), "x", false),
                        false,
                        false));

        assertThat(statement("ALTER TABLE foo.t RENAME COLUMN IF EXISTS c.d TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "c", false), new Identifier(location(1, 45), "d", false))),
                        new Identifier(location(1, 50), "x", false),
                        false,
                        true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN c.d TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "c", false), new Identifier(location(1, 45), "d", false))),
                        new Identifier(location(1, 50), "x", false),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN b.\"c.d\" TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "b", false), new Identifier(location(1, 45), "c.d", true))),
                        new Identifier(location(1, 54), "x", false),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN \"b.c\".d TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 43), "b.c", true), new Identifier(location(1, 49), "d", false))),
                        new Identifier(location(1, 54), "x", false),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t RENAME COLUMN IF EXISTS c.d TO x"))
                .isEqualTo(new RenameColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 53), "c", false), new Identifier(location(1, 55), "d", false))),
                        new Identifier(location(1, 60), "x", false),
                        true,
                        true));
    }

    @Test
    public void testRenameView()
    {
        assertStatement("ALTER VIEW a RENAME TO b", new RenameView(QualifiedName.of("a"), QualifiedName.of("b")));
    }

    @Test
    public void testAlterViewSetAuthorization()
    {
        assertStatement(
                "ALTER VIEW foo.bar.baz SET AUTHORIZATION qux",
                new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("qux"))));
        assertStatement(
                "ALTER VIEW foo.bar.baz SET AUTHORIZATION USER qux",
                new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("qux"))));
        assertStatement(
                "ALTER VIEW foo.bar.baz SET AUTHORIZATION ROLE qux",
                new SetViewAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("qux"))));
    }

    @Test
    public void testTableExecute()
    {
        Table table = new Table(QualifiedName.of("foo"));
        Identifier procedure = new Identifier("bar");

        assertStatement("ALTER TABLE foo EXECUTE bar", new TableExecute(location(1, 1), table, procedure, ImmutableList.of(), Optional.empty()));
        assertStatement(
                "ALTER TABLE foo EXECUTE bar(bah => 1, wuh => 'clap') WHERE age > 17",
                new TableExecute(
                        location(1, 1),
                        table,
                        procedure,
                        ImmutableList.of(
                                new CallArgument(identifier("bah"), new LongLiteral("1")),
                                new CallArgument(identifier("wuh"), new StringLiteral("clap"))),
                        Optional.of(
                                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                        new Identifier("age"),
                                        new LongLiteral("17")))));

        assertStatement(
                "ALTER TABLE foo EXECUTE bar(1, 'clap') WHERE age > 17",
                new TableExecute(
                        location(1, 1),
                        table,
                        procedure,
                        ImmutableList.of(
                                new CallArgument(new LongLiteral("1")),
                                new CallArgument(new StringLiteral("clap"))),
                        Optional.of(
                                new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                        new Identifier("age"),
                                        new LongLiteral("17")))));
    }

    @Test
    public void testAnalyze()
    {
        QualifiedName table = QualifiedName.of("foo");
        assertStatement("ANALYZE foo", new Analyze(table, ImmutableList.of()));

        assertStatement("ANALYZE foo WITH ( \"string\" = 'bar', \"long\" = 42, computed = concat('ban', 'ana'), a = ARRAY[ 'v1', 'v2' ] )",
                new Analyze(table, ImmutableList.of(
                        new Property(new Identifier("string"), new StringLiteral("bar")),
                        new Property(new Identifier("long"), new LongLiteral("42")),
                        new Property(
                                new Identifier("computed"),
                                new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(new StringLiteral("ban"), new StringLiteral("ana")))),
                        new Property(new Identifier("a"), new Array(ImmutableList.of(new StringLiteral("v1"), new StringLiteral("v2")))))));

        assertStatement("EXPLAIN ANALYZE foo", new Explain(new Analyze(table, ImmutableList.of()), ImmutableList.of()));
        assertStatement("EXPLAIN ANALYZE ANALYZE foo", new ExplainAnalyze(new Analyze(table, ImmutableList.of()), false));
    }

    @Test
    public void testAddColumn()
    {
        assertThat(statement("ALTER TABLE foo.t ADD COLUMN c bigint"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("c"), simpleType(location(1, 31), "bigint"), true, emptyList(), Optional.empty()), false, false));

        assertThat(statement("ALTER TABLE foo.t ADD COLUMN d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), false, false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), true, false));

        assertThat(statement("ALTER TABLE foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), false, true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN IF NOT EXISTS d double NOT NULL"))
                .ignoringLocation()
                .isEqualTo(new AddColumn(
                        QualifiedName.of("foo", "t"),
                        new ColumnDefinition(QualifiedName.of("d"), simpleType(location(1, 31), "double"), false, emptyList(), Optional.empty()), true, true));

        // Add a field
        assertThat(statement("ALTER TABLE foo.t ADD COLUMN c.d double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "t", false))),
                        new ColumnDefinition(
                                location(1, 30),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 30), "c", false), new Identifier(location(1, 32), "d", false))),
                                simpleType(location(1, 34), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        false,
                        false));

        assertThat(statement("ALTER TABLE foo.t ADD COLUMN IF NOT EXISTS c.d double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 13), "foo", false), new Identifier(location(1, 17), "t", false))),
                        new ColumnDefinition(
                                location(1, 44),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 44), "c", false), new Identifier(location(1, 46), "d", false))),
                                simpleType(location(1, 48), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        false,
                        true));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN c.d double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        new ColumnDefinition(
                                location(1, 40),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 40), "c", false), new Identifier(location(1, 42), "d", false))),
                                simpleType(location(1, 44), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN b.\"c.d\" double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        new ColumnDefinition(
                                location(1, 40),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 40), "b", false), new Identifier(location(1, 42), "c.d", true))),
                                simpleType(location(1, 48), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN \"b.c\".d double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        new ColumnDefinition(
                                location(1, 40),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 40), "b.c", true), new Identifier(location(1, 46), "d", false))),
                                simpleType(location(1, 48), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        true,
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ADD COLUMN IF NOT EXISTS c.d double"))
                .isEqualTo(new AddColumn(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 23), "foo", false), new Identifier(location(1, 27), "t", false))),
                        new ColumnDefinition(
                                location(1, 54),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 54), "c", false), new Identifier(location(1, 56), "d", false))),
                                simpleType(location(1, 58), "double"),
                                true,
                                ImmutableList.of(),
                                Optional.empty()),
                        true,
                        true));
    }

    @Test
    public void testDropColumn()
    {
        assertStatement("ALTER TABLE foo.t DROP COLUMN c", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c"), false, false));
        assertStatement("ALTER TABLE \"t x\" DROP COLUMN \"c d\"", new DropColumn(QualifiedName.of("t x"), QualifiedName.of("c d"), false, false));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP COLUMN c", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c"), true, false));
        assertStatement("ALTER TABLE foo.t DROP COLUMN IF EXISTS c", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c"), false, true));
        assertStatement("ALTER TABLE IF EXISTS foo.t DROP COLUMN IF EXISTS c", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c"), true, true));

        assertStatement("ALTER TABLE foo.t DROP COLUMN \"c.d\"", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c.d"), false, false));
        assertStatement("ALTER TABLE foo.t DROP COLUMN c.d", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("c", "d"), false, false));
        assertStatement("ALTER TABLE foo.t DROP COLUMN b.\"c.d\"", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("b", "c.d"), false, false));
        assertStatement("ALTER TABLE foo.t DROP COLUMN \"b.c\".d", new DropColumn(QualifiedName.of("foo", "t"), QualifiedName.of("b.c", "d"), false, false));
    }

    @Test
    public void testAlterColumnSetDataType()
    {
        assertThat(statement("ALTER TABLE foo.t ALTER COLUMN a SET DATA TYPE bigint"))
                .isEqualTo(new SetColumnType(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 13), "foo", false),
                                new Identifier(location(1, 17), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 32), "a", false))),
                        simpleType(location(1, 48), "bigint"),
                        false));

        assertThat(statement("ALTER TABLE IF EXISTS foo.t ALTER COLUMN b SET DATA TYPE double"))
                .isEqualTo(new SetColumnType(
                        location(1, 1),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(location(1, 23), "foo", false),
                                new Identifier(location(1, 27), "t", false))),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 42), "b", false))),
                        simpleType(location(1, 58), "double"),
                        true));
    }

    @Test
    public void testAlterTableSetAuthorization()
    {
        assertStatement(
                "ALTER TABLE foo.bar.baz SET AUTHORIZATION qux",
                new SetTableAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("qux"))));
        assertStatement(
                "ALTER TABLE foo.bar.baz SET AUTHORIZATION USER qux",
                new SetTableAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("qux"))));
        assertStatement(
                "ALTER TABLE foo.bar.baz SET AUTHORIZATION ROLE qux",
                new SetTableAuthorization(QualifiedName.of("foo", "bar", "baz"), new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("qux"))));
    }

    @Test
    public void testCreateView()
    {
        Query query = simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t")));

        assertStatement("CREATE VIEW a AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.empty(), Optional.empty()));
        assertStatement("CREATE OR REPLACE VIEW a AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, true, Optional.empty(), Optional.empty()));

        assertStatement("CREATE VIEW a SECURITY DEFINER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.empty(), Optional.of(CreateView.Security.DEFINER)));
        assertStatement("CREATE VIEW a SECURITY INVOKER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.empty(), Optional.of(CreateView.Security.INVOKER)));

        assertStatement("CREATE VIEW a COMMENT 'comment' SECURITY DEFINER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of("comment"), Optional.of(CreateView.Security.DEFINER)));
        assertStatement("CREATE VIEW a COMMENT '' SECURITY INVOKER AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of(""), Optional.of(CreateView.Security.INVOKER)));

        assertStatement("CREATE VIEW a COMMENT 'comment' AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of("comment"), Optional.empty()));
        assertStatement("CREATE VIEW a COMMENT '' AS SELECT * FROM t", new CreateView(QualifiedName.of("a"), query, false, Optional.of(""), Optional.empty()));

        assertStatement("CREATE VIEW bar.foo AS SELECT * FROM t", new CreateView(QualifiedName.of("bar", "foo"), query, false, Optional.empty(), Optional.empty()));
        assertStatement("CREATE VIEW \"awesome view\" AS SELECT * FROM t", new CreateView(QualifiedName.of("awesome view"), query, false, Optional.empty(), Optional.empty()));
        assertStatement("CREATE VIEW \"awesome schema\".\"awesome view\" AS SELECT * FROM t", new CreateView(QualifiedName.of("awesome schema", "awesome view"), query, false, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testGrant()
    {
        assertStatement("GRANT INSERT, DELETE ON t TO u",
                new Grant(
                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u")),
                        false));
        assertStatement("GRANT UPDATE ON t TO u",
                new Grant(
                        Optional.of(ImmutableList.of("UPDATE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u")),
                        false));
        assertStatement("GRANT SELECT ON t TO ROLE PUBLIC WITH GRANT OPTION",
                new Grant(
                        Optional.of(ImmutableList.of("SELECT")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("PUBLIC")),
                        true));
        assertStatement("GRANT ALL PRIVILEGES ON TABLE t TO USER u",
                new Grant(
                        Optional.empty(),
                        Optional.of(GrantOnType.TABLE),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u")),
                        false));
        assertStatement("GRANT DELETE ON \"t\" TO ROLE \"public\" WITH GRANT OPTION",
                new Grant(
                        Optional.of(ImmutableList.of("DELETE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("public")),
                        true));
        assertStatement("GRANT SELECT ON SCHEMA s TO USER u",
                new Grant(
                        Optional.of(ImmutableList.of("SELECT")),
                        Optional.of(GrantOnType.SCHEMA),
                        QualifiedName.of("s"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u")),
                        false));
    }

    @Test
    public void testDeny()
    {
        assertStatement("DENY INSERT, DELETE ON t TO u",
                new Deny(
                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("DENY UPDATE ON t TO u",
                new Deny(
                        Optional.of(ImmutableList.of("UPDATE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("DENY ALL PRIVILEGES ON TABLE t TO USER u",
                new Deny(
                        Optional.empty(),
                        Optional.of(GrantOnType.TABLE),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u"))));
        assertStatement("DENY SELECT ON SCHEMA s TO USER u",
                new Deny(
                        Optional.of(ImmutableList.of("SELECT")),
                        Optional.of(GrantOnType.SCHEMA),
                        QualifiedName.of("s"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u"))));
    }

    @Test
    public void testRevoke()
    {
        assertStatement("REVOKE INSERT, DELETE ON t FROM u",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("INSERT", "DELETE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("REVOKE UPDATE ON t FROM u",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("UPDATE")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("REVOKE GRANT OPTION FOR SELECT ON t FROM ROLE PUBLIC",
                new Revoke(
                        true,
                        Optional.of(ImmutableList.of("SELECT")),
                        Optional.empty(),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("PUBLIC"))));
        assertStatement("REVOKE ALL PRIVILEGES ON TABLE t FROM USER u",
                new Revoke(
                        false,
                        Optional.empty(),
                        Optional.of(GrantOnType.TABLE),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u"))));
        assertStatement("REVOKE DELETE ON TABLE \"t\" FROM \"u\"",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("DELETE")),
                        Optional.of(GrantOnType.TABLE),
                        QualifiedName.of("t"),
                        new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("u"))));
        assertStatement("REVOKE SELECT ON SCHEMA s FROM USER u",
                new Revoke(
                        false,
                        Optional.of(ImmutableList.of("SELECT")),
                        Optional.of(GrantOnType.SCHEMA),
                        QualifiedName.of("s"),
                        new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("u"))));
    }

    @Test
    public void testShowGrants()
    {
        assertStatement("SHOW GRANTS ON TABLE t",
                new ShowGrants(true, Optional.of(QualifiedName.of("t"))));
        assertStatement("SHOW GRANTS ON t",
                new ShowGrants(false, Optional.of(QualifiedName.of("t"))));
        assertStatement("SHOW GRANTS",
                new ShowGrants(false, Optional.empty()));
    }

    @Test
    public void testShowRoles()
    {
        assertStatement("SHOW ROLES",
                new ShowRoles(Optional.empty(), false));
        assertStatement("SHOW ROLES FROM foo",
                new ShowRoles(Optional.of(new Identifier("foo")), false));
        assertStatement("SHOW ROLES IN foo",
                new ShowRoles(Optional.of(new Identifier("foo")), false));

        assertStatement("SHOW CURRENT ROLES",
                new ShowRoles(Optional.empty(), true));
        assertStatement("SHOW CURRENT ROLES FROM foo",
                new ShowRoles(Optional.of(new Identifier("foo")), true));
        assertStatement("SHOW CURRENT ROLES IN foo",
                new ShowRoles(Optional.of(new Identifier("foo")), true));
    }

    @Test
    public void testShowRoleGrants()
    {
        assertStatement("SHOW ROLE GRANTS",
                new ShowRoleGrants(Optional.empty(), Optional.empty()));
        assertStatement("SHOW ROLE GRANTS FROM catalog",
                new ShowRoleGrants(Optional.of(new Identifier("catalog"))));
    }

    @Test
    public void testSetPath()
    {
        assertStatement("SET PATH iLikeToEat.apples, andBananas",
                new SetPath(new PathSpecification(Optional.empty(), ImmutableList.of(
                        new PathElement(Optional.of(new Identifier("iLikeToEat")), new Identifier("apples")),
                        new PathElement(Optional.empty(), new Identifier("andBananas"))))));

        assertStatement("SET PATH \"schemas,with\".\"grammar.in\", \"their!names\"",
                new SetPath(new PathSpecification(Optional.empty(), ImmutableList.of(
                        new PathElement(Optional.of(new Identifier("schemas,with")), new Identifier("grammar.in")),
                        new PathElement(Optional.empty(), new Identifier("their!names"))))));

        assertThatThrownBy(() -> assertStatement("SET PATH one.too.many, qualifiers",
                new SetPath(new PathSpecification(Optional.empty(), ImmutableList.of(
                        new PathElement(Optional.empty(), new Identifier("dummyValue")))))))
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:17: mismatched input '.'. Expecting: ',', <EOF>");

        assertThatThrownBy(() -> SQL_PARSER.createStatement("SET PATH ", new ParsingOptions()))
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:10: mismatched input '<EOF>'. Expecting: <identifier>");
    }

    @Test
    public void testSetTimeZone()
    {
        assertThat(statement("SET TIME ZONE LOCAL"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.empty()));
        assertThat(statement("SET TIME ZONE 'America/Los_Angeles'"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.of(new StringLiteral(
                                        location(1, 15),
                                        "America/Los_Angeles"))));
        assertThat(statement("SET TIME ZONE concat_ws('/', 'America', 'Los_Angeles')"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.of(new FunctionCall(
                                        location(1, 15),
                                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 15), "concat_ws", false))),
                                        ImmutableList.of(
                                                new StringLiteral(
                                                        location(1, 25),
                                                        "/"),
                                                new StringLiteral(
                                                        location(1, 30),
                                                        "America"),
                                                new StringLiteral(
                                                        location(1, 41),
                                                        "Los_Angeles"))))));
        assertThat(statement("SET TIME ZONE '-08:00'"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.of(new StringLiteral(
                                        location(1, 15),
                                        "-08:00"))));
        assertThat(statement("SET TIME ZONE INTERVAL '10' HOUR"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.of(new IntervalLiteral(
                                        location(1, 15),
                                        "10", Sign.POSITIVE, IntervalField.HOUR, Optional.empty()))));
        assertThat(statement("SET TIME ZONE INTERVAL -'08:00' HOUR TO MINUTE"))
                .isEqualTo(
                        new SetTimeZone(
                                location(1, 1),
                                Optional.of(new IntervalLiteral(
                                        location(1, 15), "08:00", Sign.NEGATIVE, IntervalField.HOUR, Optional.of(IntervalField.MINUTE)))));
    }

    @Test
    public void testWith()
    {
        assertStatement("WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM y) TABLE z",
                new Query(
                        Optional.of(new With(false, ImmutableList.of(
                                new WithQuery(
                                        identifier("a"),
                                        simpleQuery(
                                                selectList(new AllColumns()),
                                                table(QualifiedName.of("x"))),
                                        Optional.of(ImmutableList.of(identifier("t"), identifier("u")))),
                                new WithQuery(
                                        identifier("b"),
                                        simpleQuery(
                                                selectList(new AllColumns()),
                                                table(QualifiedName.of("y"))),
                                        Optional.empty())))),
                        new Table(QualifiedName.of("z")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));

        assertStatement("WITH RECURSIVE a AS (SELECT * FROM x) TABLE y",
                new Query(
                        Optional.of(new With(true, ImmutableList.of(
                                new WithQuery(
                                        identifier("a"),
                                        simpleQuery(selectList(new AllColumns()),
                                                table(QualifiedName.of("x"))),
                                        Optional.empty())))),
                        new Table(QualifiedName.of("y")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testImplicitJoin()
    {
        assertStatement("SELECT * FROM a, b",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.empty())));
    }

    @Test
    public void testExplain()
    {
        assertStatement("EXPLAIN SELECT * FROM t",
                new Explain(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), ImmutableList.of()));
        assertStatement("EXPLAIN (TYPE LOGICAL) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableList.of(new ExplainType(ExplainType.Type.LOGICAL))));
        assertStatement("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) SELECT * FROM t",
                new Explain(
                        simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))),
                        ImmutableList.of(
                                new ExplainType(ExplainType.Type.LOGICAL),
                                new ExplainFormat(ExplainFormat.Type.TEXT))));

        assertStatementIsInvalid("EXPLAIN VERBOSE SELECT * FROM t")
                .withMessageStartingWith("line 1:9: mismatched input 'VERBOSE'. Expecting: '(', 'ALTER', 'ANALYZE', 'CALL',");

        assertStatementIsInvalid("EXPLAIN VERBOSE (type LOGICAL) SELECT * FROM t")
                .withMessageStartingWith("line 1:9: mismatched input 'VERBOSE'. Expecting: '(', 'ALTER', 'ANALYZE', 'CALL',");
    }

    @Test
    public void testExplainAnalyze()
    {
        assertStatement("EXPLAIN ANALYZE SELECT * FROM t",
                new ExplainAnalyze(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), false));

        assertStatement("EXPLAIN ANALYZE VERBOSE SELECT * FROM t",
                new ExplainAnalyze(simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("t"))), true));

        assertStatementIsInvalid("EXPLAIN ANALYZE (type DISTRIBUTED) SELECT * FROM t")
                .withMessage("line 1:18: mismatched input 'type'. Expecting: '(', 'SELECT', 'TABLE', 'VALUES'");

        assertStatementIsInvalid("EXPLAIN ANALYZE VERBOSE (type DISTRIBUTED) SELECT * FROM t")
                .withMessage("line 1:26: mismatched input 'type'. Expecting: '(', 'SELECT', 'TABLE', 'VALUES'");
    }

    @Test
    public void testJoinPrecedence()
    {
        assertStatement("SELECT * FROM a CROSS JOIN b LEFT JOIN c ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.LEFT,
                                new Join(
                                        Join.Type.CROSS,
                                        new Table(QualifiedName.of("a")),
                                        new Table(QualifiedName.of("b")),
                                        Optional.empty()),
                                new Table(QualifiedName.of("c")),
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
        assertStatement("SELECT * FROM a CROSS JOIN b NATURAL JOIN c CROSS JOIN d NATURAL JOIN e",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.INNER,
                                new Join(
                                        Join.Type.CROSS,
                                        new Join(
                                                Join.Type.INNER,
                                                new Join(
                                                        Join.Type.CROSS,
                                                        new Table(QualifiedName.of("a")),
                                                        new Table(QualifiedName.of("b")),
                                                        Optional.empty()),
                                                new Table(QualifiedName.of("c")),
                                                Optional.of(new NaturalJoin())),
                                        new Table(QualifiedName.of("d")),
                                        Optional.empty()),
                                new Table(QualifiedName.of("e")),
                                Optional.of(new NaturalJoin()))));
    }

    @Test
    public void testUnnest()
    {
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new Identifier("a")), false),
                                Optional.empty())));
        assertStatement("SELECT * FROM t CROSS JOIN UNNEST(a, b) WITH ORDINALITY",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                new Unnest(ImmutableList.of(new Identifier("a"), new Identifier("b")), true),
                                Optional.empty())));
        assertStatement("SELECT * FROM t FULL JOIN UNNEST(a) AS tmp (c) ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.FULL,
                                new Table(QualifiedName.of("t")),
                                new AliasedRelation(new Unnest(ImmutableList.of(new Identifier("a")), false), new Identifier("tmp"), ImmutableList.of(new Identifier("c"))),
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testLateral()
    {
        Lateral lateralRelation = new Lateral(
                query(new Values(ImmutableList.of(new LongLiteral("1")))));

        assertStatement("SELECT * FROM t, LATERAL (VALUES 1) a(x)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.IMPLICIT,
                                new Table(QualifiedName.of("t")),
                                new AliasedRelation(lateralRelation, identifier("a"), ImmutableList.of(identifier("x"))),
                                Optional.empty())));

        assertStatement("SELECT * FROM t CROSS JOIN LATERAL (VALUES 1) ",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.CROSS,
                                new Table(QualifiedName.of("t")),
                                lateralRelation,
                                Optional.empty())));

        assertStatement("SELECT * FROM t FULL JOIN LATERAL (VALUES 1) ON true",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.FULL,
                                new Table(QualifiedName.of("t")),
                                lateralRelation,
                                Optional.of(new JoinOn(BooleanLiteral.TRUE_LITERAL)))));
    }

    @Test
    public void testStartTransaction()
    {
        assertStatement("START TRANSACTION",
                new StartTransaction(ImmutableList.of()));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_UNCOMMITTED))));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ COMMITTED",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_COMMITTED))));
        assertStatement("START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.REPEATABLE_READ))));
        assertStatement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.SERIALIZABLE))));
        assertStatement("START TRANSACTION READ ONLY",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(true))));
        assertStatement("START TRANSACTION READ WRITE",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(false))));
        assertStatement("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
                new StartTransaction(ImmutableList.of(
                        new Isolation(Isolation.Level.READ_COMMITTED),
                        new TransactionAccessMode(true))));
        assertStatement("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(true),
                        new Isolation(Isolation.Level.READ_COMMITTED))));
        assertStatement("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE",
                new StartTransaction(ImmutableList.of(
                        new TransactionAccessMode(false),
                        new Isolation(Isolation.Level.SERIALIZABLE))));
    }

    @Test
    public void testCommit()
    {
        assertStatement("COMMIT", new Commit());
        assertStatement("COMMIT WORK", new Commit());
    }

    @Test
    public void testRollback()
    {
        assertStatement("ROLLBACK", new Rollback());
        assertStatement("ROLLBACK WORK", new Rollback());
    }

    @Test
    public void testAtTimeZone()
    {
        assertStatement("SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'",
                simpleQuery(selectList(
                        new AtTimeZone(new TimestampLiteral("2012-10-31 01:00 UTC"), new StringLiteral("America/Los_Angeles")))));
    }

    @Test
    public void testLambda()
    {
        assertThat(expression("() -> x"))
                .isEqualTo(new LambdaExpression(
                        location(1, 1),
                        ImmutableList.of(),
                        new Identifier(location(1, 7), "x", false)));
        assertThat(expression("x -> sin(x)"))
                .isEqualTo(new LambdaExpression(
                        location(1, 1),
                        ImmutableList.of(new LambdaArgumentDeclaration(new Identifier(location(1, 1), "x", false))),
                        new FunctionCall(
                                location(1, 6),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 6), "sin", false))),
                                ImmutableList.of(new Identifier(location(1, 10), "x", false)))));
        assertThat(expression("(x, y) -> mod(x, y)"))
                .isEqualTo(new LambdaExpression(
                        location(1, 1),
                        ImmutableList.of(
                                new LambdaArgumentDeclaration(new Identifier(location(1, 2), "x", false)),
                                new LambdaArgumentDeclaration(new Identifier(location(1, 5), "y", false))),
                        new FunctionCall(
                                location(1, 11),
                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 11), "mod", false))),
                                ImmutableList.of(
                                        new Identifier(location(1, 15), "x", false),
                                        new Identifier(location(1, 18), "y", false)))));
    }

    @Test
    public void testNonReserved()
    {
        assertStatement("SELECT zone FROM t",
                simpleQuery(
                        selectList(new Identifier("zone")),
                        table(QualifiedName.of("t"))));
        assertStatement("SELECT INCLUDING, EXCLUDING, PROPERTIES FROM t",
                simpleQuery(
                        selectList(
                                new Identifier("INCLUDING"),
                                new Identifier("EXCLUDING"),
                                new Identifier("PROPERTIES")),
                        table(QualifiedName.of("t"))));
        assertStatement("SELECT ALL, SOME, ANY FROM t",
                simpleQuery(
                        selectList(
                                new Identifier("ALL"),
                                new Identifier("SOME"),
                                new Identifier("ANY")),
                        table(QualifiedName.of("t"))));

        NodeLocation location = new NodeLocation(1, 1);
        assertThat(expression("stats"))
                .isEqualTo(new Identifier(location, "stats", false));
        assertThat(expression("nfd"))
                .isEqualTo(new Identifier(location, "nfd", false));
        assertThat(expression("nfc"))
                .isEqualTo(new Identifier(location, "nfc", false));
        assertThat(expression("nfkd"))
                .isEqualTo(new Identifier(location, "nfkd", false));
        assertThat(expression("nfkc"))
                .isEqualTo(new Identifier(location, "nfkc", false));
    }

    @Test
    public void testBinaryLiteralToHex()
    {
        // note that toHexString() always outputs in upper case
        assertThat(new BinaryLiteral("ab 01").toHexString())
                .isEqualTo("AB01");
    }

    @Test
    public void testCall()
    {
        assertStatement("CALL foo()", new Call(QualifiedName.of("foo"), ImmutableList.of()));
        assertStatement("CALL foo(123, a => 1, b => 'go', 456)", new Call(QualifiedName.of("foo"), ImmutableList.of(
                new CallArgument(new LongLiteral("123")),
                new CallArgument(identifier("a"), new LongLiteral("1")),
                new CallArgument(identifier("b"), new StringLiteral("go")),
                new CallArgument(new LongLiteral("456")))));
    }

    @Test
    public void testPrepare()
    {
        assertStatement("PREPARE myquery FROM select * from foo",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new AllColumns()),
                        table(QualifiedName.of("foo")))));
    }

    @Test
    public void testPrepareDropView()
    {
        assertStatement("PREPARE statement1 FROM DROP VIEW IF EXISTS \"catalog-test\".\"test\".\"foo\"",
                new Prepare(identifier("statement1"),
                        new DropView(QualifiedName.of("catalog-test", "test", "foo"), true)));
        assertStatementIsInvalid("PREPARE statement1 FROM DROP VIEW IF EXISTS catalog-test.test.foo")
                .withMessage("line 1:52: mismatched input '-'. Expecting: '.', <EOF>");
    }

    @Test
    public void testPrepareWithParameters()
    {
        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")))));

        assertStatement("PREPARE myquery FROM SELECT * FROM foo LIMIT ?",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new AllColumns()),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(new Parameter(0))))));

        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo LIMIT ?",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Limit(new Parameter(2))))));

        assertStatement("PREPARE myquery FROM SELECT ? FROM foo FETCH FIRST ? ROWS ONLY",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(new Parameter(1))))));

        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo FETCH NEXT ? ROWS WITH TIES",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new FetchFirst(new Parameter(2), true)))));

        assertStatement("PREPARE myquery FROM SELECT ?, ? FROM foo OFFSET ? ROWS",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0), new Parameter(1)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new Parameter(2))),
                        Optional.empty())));

        assertStatement("PREPARE myquery FROM SELECT ? FROM foo OFFSET ? ROWS LIMIT ?",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new Parameter(1))),
                        Optional.of(new Limit(new Parameter(2))))));

        assertStatement("PREPARE myquery FROM SELECT ? FROM foo OFFSET ? ROWS FETCH FIRST ? ROWS WITH TIES",
                new Prepare(identifier("myquery"), simpleQuery(
                        selectList(new Parameter(0)),
                        table(QualifiedName.of("foo")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Offset(new Parameter(1))),
                        Optional.of(new FetchFirst(new Parameter(2), true)))));
    }

    @Test
    public void testDeallocatePrepare()
    {
        assertStatement("DEALLOCATE PREPARE myquery", new Deallocate(identifier("myquery")));
    }

    @Test
    public void testExecute()
    {
        assertStatement("EXECUTE myquery", new Execute(identifier("myquery"), emptyList()));
    }

    @Test
    public void testExecuteWithUsing()
    {
        assertStatement("EXECUTE myquery USING 1, 'abc', ARRAY ['hello']",
                new Execute(identifier("myquery"), ImmutableList.of(new LongLiteral("1"), new StringLiteral("abc"), new Array(ImmutableList.of(new StringLiteral("hello"))))));
    }

    @Test
    public void testExecuteImmediate()
    {
        assertStatement(
                "EXECUTE IMMEDIATE 'SELECT * FROM foo'",
                new ExecuteImmediate(
                        new NodeLocation(1, 1),
                        new StringLiteral(new NodeLocation(1, 19), "SELECT * FROM foo"),
                        emptyList()));
    }

    @Test
    public void testExecuteImmediateWithUsing()
    {
        assertStatement(
                "EXECUTE IMMEDIATE 'SELECT ?, ? FROM foo' USING 1, 'abc', ARRAY ['hello']",
                new ExecuteImmediate(
                        new NodeLocation(1, 1),
                        new StringLiteral(new NodeLocation(1, 19), "SELECT ?, ? FROM foo"),
                        ImmutableList.of(new LongLiteral("1"), new StringLiteral("abc"), new Array(ImmutableList.of(new StringLiteral("hello"))))));
    }

    @Test
    public void testExists()
    {
        assertStatement("SELECT EXISTS(SELECT 1)", simpleQuery(selectList(exists(simpleQuery(selectList(new LongLiteral("1")))))));

        assertStatement(
                "SELECT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(new ComparisonExpression(
                                ComparisonExpression.Operator.EQUAL,
                                exists(simpleQuery(selectList(new LongLiteral("1")))),
                                exists(simpleQuery(selectList(new LongLiteral("2"))))))));

        assertStatement(
                "SELECT NOT EXISTS(SELECT 1) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new NotExpression(
                                        new ComparisonExpression(
                                                ComparisonExpression.Operator.EQUAL,
                                                exists(simpleQuery(selectList(new LongLiteral("1")))),
                                                exists(simpleQuery(selectList(new LongLiteral("2")))))))));

        assertStatement(
                "SELECT (NOT EXISTS(SELECT 1)) = EXISTS(SELECT 2)",
                simpleQuery(
                        selectList(
                                new ComparisonExpression(
                                        ComparisonExpression.Operator.EQUAL,
                                        new NotExpression(exists(simpleQuery(selectList(new LongLiteral("1"))))),
                                        exists(simpleQuery(selectList(new LongLiteral("2"))))))));
    }

    private static ExistsPredicate exists(Query query)
    {
        return new ExistsPredicate(new SubqueryExpression(query));
    }

    @Test
    public void testShowStats()
    {
        String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = makeQualifiedName(fullName);
            assertStatement(format("SHOW STATS FOR %s", qualifiedName), new ShowStats(new Table(qualifiedName)));
        }
    }

    @Test
    public void testShowStatsForQuery()
    {
        String[] tableNames = {"t", "s.t", "c.s.t"};

        for (String fullName : tableNames) {
            QualifiedName qualifiedName = makeQualifiedName(fullName);

            // Simple SELECT
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s)", qualifiedName),
                    createShowStats(qualifiedName, ImmutableList.of(new AllColumns()), Optional.empty()));

            // SELECT with predicate
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0)", qualifiedName),
                    createShowStats(qualifiedName,
                            ImmutableList.of(new AllColumns()),
                            Optional.of(
                                    new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                            new Identifier("field"),
                                            new LongLiteral("0")))));

            // SELECT with more complex predicate
            assertStatement(format("SHOW STATS FOR (SELECT * FROM %s WHERE field > 0 or field < 0)", qualifiedName),
                    createShowStats(qualifiedName,
                            ImmutableList.of(new AllColumns()),
                            Optional.of(
                                    LogicalExpression.or(
                                            new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                                    new Identifier("field"),
                                                    new LongLiteral("0")),
                                            new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN,
                                                    new Identifier("field"),
                                                    new LongLiteral("0"))))));
        }

        // SELECT with LIMIT
        assertThat(statement("SHOW STATS FOR (SELECT * FROM t LIMIT 10)"))
                .isEqualTo(
                        new ShowStats(
                                Optional.of(location(1, 1)),
                                new TableSubquery(
                                        new Query(
                                                location(1, 17),
                                                Optional.empty(),
                                                new QuerySpecification(
                                                        location(1, 17),
                                                        new Select(
                                                                location(1, 17),
                                                                false,
                                                                ImmutableList.of(new AllColumns(location(1, 24), Optional.empty(), ImmutableList.of()))),
                                                        Optional.of(new Table(
                                                                location(1, 31),
                                                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 31), "t", false))))),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        ImmutableList.of(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.of(new Limit(location(1, 33), new LongLiteral(location(1, 39), "10")))),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty()))));

        // SELECT with ORDER BY ... LIMIT
        assertThat(statement("SHOW STATS FOR (SELECT * FROM t ORDER BY field LIMIT 10)"))
                .isEqualTo(
                        new ShowStats(
                                Optional.of(location(1, 1)),
                                new TableSubquery(
                                        new Query(
                                                location(1, 17),
                                                Optional.empty(),
                                                new QuerySpecification(
                                                        location(1, 17),
                                                        new Select(
                                                                location(1, 17),
                                                                false,
                                                                ImmutableList.of(new AllColumns(location(1, 24), Optional.empty(), ImmutableList.of()))),
                                                        Optional.of(new Table(
                                                                location(1, 31),
                                                                QualifiedName.of(ImmutableList.of(new Identifier(location(1, 31), "t", false))))),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        ImmutableList.of(),
                                                        Optional.of(new OrderBy(location(1, 33), ImmutableList.of(
                                                                new SortItem(location(1, 42), new Identifier(location(1, 42), "field", false), ASCENDING, UNDEFINED)))),
                                                        Optional.empty(),
                                                        Optional.of(new Limit(location(1, 48), new LongLiteral(location(1, 54), "10")))),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty()))));

        // SELECT with WITH
        assertThat(statement("""
                SHOW STATS FOR (
                   WITH t AS (SELECT 1 )
                   SELECT * FROM t)
                """))
                .isEqualTo(
                        new ShowStats(
                                Optional.of(location(1, 1)),
                                new TableSubquery(
                                        new Query(
                                                location(2, 4),
                                                Optional.of(
                                                        new With(
                                                                location(2, 4),
                                                                false,
                                                                ImmutableList.of(
                                                                        new WithQuery(
                                                                                location(2, 9),
                                                                                new Identifier(location(2, 9), "t", false),
                                                                                new Query(
                                                                                        location(2, 15),
                                                                                        Optional.empty(),
                                                                                        new QuerySpecification(
                                                                                                location(2, 15),
                                                                                                new Select(
                                                                                                        location(2, 15),
                                                                                                        false,
                                                                                                        ImmutableList.of(
                                                                                                                new SingleColumn(
                                                                                                                        location(2, 22),
                                                                                                                        new LongLiteral(location(2, 22), "1"),
                                                                                                                        Optional.empty()))),
                                                                                                Optional.empty(),
                                                                                                Optional.empty(),
                                                                                                Optional.empty(),
                                                                                                Optional.empty(),
                                                                                                ImmutableList.of(),
                                                                                                Optional.empty(),
                                                                                                Optional.empty(),
                                                                                                Optional.empty()),
                                                                                        Optional.empty(),
                                                                                        Optional.empty(),
                                                                                        Optional.empty()),
                                                                                Optional.empty())))),
                                                new QuerySpecification(
                                                        location(3, 4),
                                                        new Select(
                                                                location(3, 4),
                                                                false,
                                                                ImmutableList.of(new AllColumns(location(3, 11), Optional.empty(), ImmutableList.of()))),
                                                        Optional.of(new Table(
                                                                location(3, 18),
                                                                QualifiedName.of(ImmutableList.of(new Identifier(location(3, 18), "t", false))))),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        ImmutableList.of(),
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty()),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty()))));
    }

    private static ShowStats createShowStats(QualifiedName name, List<SelectItem> selects, Optional<Expression> where)
    {
        return new ShowStats(
                new TableSubquery(simpleQuery(new Select(false, selects),
                        new Table(name),
                        where,
                        Optional.empty())));
    }

    @Test
    public void testDescribeOutput()
    {
        assertStatement("DESCRIBE OUTPUT myquery", new DescribeOutput(identifier("myquery")));
    }

    @Test
    public void testDescribeInput()
    {
        assertStatement("DESCRIBE INPUT myquery", new DescribeInput(identifier("myquery")));
    }

    @Test
    public void testAggregationFilter()
    {
        assertStatement("SELECT SUM(x) FILTER (WHERE x > 4)",
                simpleQuery(selectList(
                        new FunctionCall(
                                Optional.empty(),
                                QualifiedName.of("SUM"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(
                                        ComparisonExpression.Operator.GREATER_THAN,
                                        new Identifier("x"),
                                        new LongLiteral("4"))),
                                Optional.empty(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x"))))));
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertExpression("col1 < ANY (SELECT col2 FROM table1)",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN,
                        QuantifiedComparisonExpression.Quantifier.ANY,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new SingleColumn(identifier("col2"))), table(QualifiedName.of("table1"))))));
        assertExpression("col1 = ALL (VALUES ROW(1), ROW(2))",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        QuantifiedComparisonExpression.Quantifier.ALL,
                        identifier("col1"),
                        new SubqueryExpression(query(values(row(new LongLiteral("1")), row(new LongLiteral("2")))))));
        assertExpression("col1 >= SOME (SELECT 10)",
                new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                        QuantifiedComparisonExpression.Quantifier.SOME,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new LongLiteral("10"))))));
    }

    @Test
    public void testAggregationWithOrderBy()
    {
        assertExpression("array_agg(x ORDER BY x DESC)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("array_agg"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(identifier("x"), DESCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(identifier("x"))));
        assertStatement("SELECT array_agg(x ORDER BY t.y) FROM t",
                simpleQuery(
                        selectList(new FunctionCall(
                                Optional.empty(),
                                QualifiedName.of("array_agg"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new DereferenceExpression(new Identifier("t"), identifier("y")), ASCENDING, UNDEFINED)))),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x")))),
                        table(QualifiedName.of("t"))));
    }

    @Test
    public void testCreateRole()
    {
        assertStatement("CREATE ROLE role", new CreateRole(new Identifier("role"), Optional.empty(), Optional.empty()));
        assertStatement("CREATE ROLE role1 WITH ADMIN admin",
                new CreateRole(
                        new Identifier("role1"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE \"role\" WITH ADMIN \"admin\"",
                new CreateRole(
                        new Identifier("role"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE \"ro le\" WITH ADMIN \"ad min\"",
                new CreateRole(
                        new Identifier("ro le"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ad min"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE \"!@#$%^&*'\" WITH ADMIN \"ад\"\"мін\"",
                new CreateRole(
                        new Identifier("!@#$%^&*'"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("ад\"мін"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE role2 WITH ADMIN USER admin1",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin1"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE role2 WITH ADMIN ROLE role1",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role1"))))),
                        Optional.empty()));
        assertStatement("CREATE ROLE role2 WITH ADMIN CURRENT_USER",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.CURRENT_USER,
                                Optional.empty())),
                        Optional.empty()));
        assertStatement("CREATE ROLE role2 WITH ADMIN CURRENT_ROLE",
                new CreateRole(
                        new Identifier("role2"),
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.CURRENT_ROLE,
                                Optional.empty())),
                        Optional.empty()));
        assertStatement("CREATE ROLE role IN my_catalog",
                new CreateRole(
                        new Identifier("role"),
                        Optional.empty(),
                        Optional.of(new Identifier("my_catalog"))));
    }

    @Test
    public void testDropRole()
    {
        assertStatement("DROP ROLE role", new DropRole(new Identifier("role"), Optional.empty()));
        assertStatement("DROP ROLE \"role\"", new DropRole(new Identifier("role"), Optional.empty()));
        assertStatement("DROP ROLE \"ro le\"", new DropRole(new Identifier("ro le"), Optional.empty()));
        assertStatement("DROP ROLE \"!@#$%^&*'ад\"\"мін\"", new DropRole(new Identifier("!@#$%^&*'ад\"мін"), Optional.empty()));
        assertStatement("DROP ROLE role IN my_catalog", new DropRole(new Identifier("role"), Optional.of(new Identifier("my_catalog"))));
    }

    @Test
    public void testGrantRoles()
    {
        assertStatement("GRANT role1 TO user1",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty(),
                        Optional.empty()));
        assertStatement("GRANT role1, role2, role3 TO user1, USER user2, ROLE role4 WITH ADMIN OPTION",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                        ImmutableSet.of(
                                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                        true,
                        Optional.empty(),
                        Optional.empty()));
        assertStatement("GRANT role1 TO user1 WITH ADMIN OPTION GRANTED BY admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("GRANT role1 TO USER user1 WITH ADMIN OPTION GRANTED BY USER admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("GRANT role1 TO ROLE role2 WITH ADMIN OPTION GRANTED BY ROLE admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("GRANT role1 TO ROLE role2 GRANTED BY ROLE admin",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("GRANT \"role1\" TO ROLE \"role2\" GRANTED BY ROLE \"admin\"",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("GRANT role1 TO user1 IN my_catalog",
                new GrantRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty(),
                        Optional.of(new Identifier("my_catalog"))));
    }

    @Test
    public void testRevokeRoles()
    {
        assertStatement("REVOKE role1 FROM user1",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty(),
                        Optional.empty()));
        assertStatement("REVOKE ADMIN OPTION FOR role1, role2, role3 FROM user1, USER user2, ROLE role4",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1"), new Identifier("role2"), new Identifier("role3")),
                        ImmutableSet.of(
                                new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1")),
                                new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user2")),
                                new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role4"))),
                        true,
                        Optional.empty(),
                        Optional.empty()));
        assertStatement("REVOKE ADMIN OPTION FOR role1 FROM user1 GRANTED BY admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("REVOKE ADMIN OPTION FOR role1 FROM USER user1 GRANTED BY USER admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("user1"))),
                        true,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.USER, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("REVOKE role1 FROM ROLE role2 GRANTED BY ROLE admin",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("REVOKE \"role1\" FROM ROLE \"role2\" GRANTED BY ROLE \"admin\"",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("role2"))),
                        false,
                        Optional.of(new GrantorSpecification(
                                GrantorSpecification.Type.PRINCIPAL,
                                Optional.of(new PrincipalSpecification(PrincipalSpecification.Type.ROLE, new Identifier("admin"))))),
                        Optional.empty()));
        assertStatement("REVOKE role1 FROM user1 IN my_catalog",
                new RevokeRoles(
                        ImmutableSet.of(new Identifier("role1")),
                        ImmutableSet.of(new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, new Identifier("user1"))),
                        false,
                        Optional.empty(),
                        Optional.of(new Identifier("my_catalog"))));
    }

    @Test
    public void testSetRole()
    {
        assertStatement("SET ROLE ALL", new SetRole(SetRole.Type.ALL, Optional.empty(), Optional.empty()));
        assertStatement("SET ROLE NONE", new SetRole(SetRole.Type.NONE, Optional.empty(), Optional.empty()));
        assertStatement("SET ROLE role", new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role")), Optional.empty()));
        assertStatement("SET ROLE \"role\"", new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role")), Optional.empty()));
        assertStatement("SET ROLE role IN my_catalog", new SetRole(SetRole.Type.ROLE, Optional.of(new Identifier("role")), Optional.of(new Identifier("my_catalog"))));
    }

    @Test
    public void testCreateMaterializedView()
    {
        // basic
        assertThat(statement("CREATE MATERIALIZED VIEW a AS SELECT * FROM t"))
                .isEqualTo(new CreateMaterializedView(
                        Optional.of(new NodeLocation(1, 1)),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 26), "a", false))),
                        new Query(
                                new NodeLocation(1, 31),
                                Optional.empty(),
                                new QuerySpecification(
                                        new NodeLocation(1, 31),
                                        new Select(
                                                new NodeLocation(1, 31),
                                                false,
                                                ImmutableList.of(new AllColumns(new NodeLocation(1, 38), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(
                                                new NodeLocation(1, 45),
                                                QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 45), "t", false))))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        false,
                        false,
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty()));

        // OR REPLACE, COMMENT
        assertThat(statement("CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A simple materialized view'" +
                " AS SELECT * FROM catalog2.schema2.tab"))
                .isEqualTo(new CreateMaterializedView(
                        Optional.of(new NodeLocation(1, 1)),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(new NodeLocation(1, 37), "catalog", false),
                                new Identifier(new NodeLocation(1, 45), "schema", false),
                                new Identifier(new NodeLocation(1, 52), "matview", false))),
                        new Query(
                                new NodeLocation(1, 100),
                                Optional.empty(),
                                new QuerySpecification(
                                        new NodeLocation(1, 100),
                                        new Select(
                                                new NodeLocation(1, 100),
                                                false,
                                                ImmutableList.of(new AllColumns(new NodeLocation(1, 107), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(
                                                new NodeLocation(1, 114),
                                                QualifiedName.of(ImmutableList.of(
                                                        new Identifier(new NodeLocation(1, 114), "catalog2", false),
                                                        new Identifier(new NodeLocation(1, 123), "schema2", false),
                                                        new Identifier(new NodeLocation(1, 131), "tab", false))))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        true,
                        false,
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.of("A simple materialized view")));

        // GRACE PERIOD
        assertThat(statement("CREATE MATERIALIZED VIEW a GRACE PERIOD INTERVAL '2' DAY AS SELECT * FROM t"))
                .isEqualTo(new CreateMaterializedView(
                        Optional.of(new NodeLocation(1, 1)),
                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 26), "a", false))),
                        new Query(
                                new NodeLocation(1, 61),
                                Optional.empty(),
                                new QuerySpecification(
                                        new NodeLocation(1, 61),
                                        new Select(
                                                new NodeLocation(1, 61),
                                                false,
                                                ImmutableList.of(new AllColumns(new NodeLocation(1, 68), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(
                                                new NodeLocation(1, 75),
                                                QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(1, 75), "t", false))))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        false,
                        false,
                        Optional.of(new IntervalLiteral(new NodeLocation(1, 41), "2", Sign.POSITIVE, IntervalField.DAY, Optional.empty())),
                        ImmutableList.of(),
                        Optional.empty()));

        // OR REPLACE, COMMENT, WITH properties
        assertThat(statement("""
                CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A simple materialized view'
                WITH (partitioned_by = ARRAY ['dateint'])
                 AS SELECT * FROM catalog2.schema2.tab
                """))
                .isEqualTo(new CreateMaterializedView(
                        Optional.of(new NodeLocation(1, 1)),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(new NodeLocation(1, 37), "catalog", false),
                                new Identifier(new NodeLocation(1, 45), "schema", false),
                                new Identifier(new NodeLocation(1, 52), "matview", false))),
                        new Query(
                                new NodeLocation(3, 5),
                                Optional.empty(),
                                new QuerySpecification(
                                        new NodeLocation(3, 5),
                                        new Select(
                                                new NodeLocation(3, 5),
                                                false,
                                                ImmutableList.of(new AllColumns(new NodeLocation(3, 12), Optional.empty(), ImmutableList.of()))),
                                        Optional.of(new Table(
                                                new NodeLocation(3, 19),
                                                QualifiedName.of(ImmutableList.of(
                                                        new Identifier(new NodeLocation(3, 19), "catalog2", false),
                                                        new Identifier(new NodeLocation(3, 28), "schema2", false),
                                                        new Identifier(new NodeLocation(3, 36), "tab", false))))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        true,
                        false,
                        Optional.empty(),
                        ImmutableList.of(new Property(
                                new NodeLocation(2, 7),
                                new Identifier(new NodeLocation(2, 7), "partitioned_by", false),
                                new Array(
                                        new NodeLocation(2, 24),
                                        ImmutableList.of(new StringLiteral(new NodeLocation(2, 31), "dateint"))))),
                        Optional.of("A simple materialized view")));

        // OR REPLACE, COMMENT, WITH properties, view text containing WITH clause
        assertThat(statement("""
                CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.matview COMMENT 'A partitioned materialized view'
                WITH (partitioned_by = ARRAY ['dateint'])
                 AS WITH a (t, u) AS (SELECT * FROM x), b AS (SELECT * FROM a) TABLE b
                """))
                .isEqualTo(new CreateMaterializedView(
                        Optional.of(new NodeLocation(1, 1)),
                        QualifiedName.of(ImmutableList.of(
                                new Identifier(new NodeLocation(1, 37), "catalog", false),
                                new Identifier(new NodeLocation(1, 45), "schema", false),
                                new Identifier(new NodeLocation(1, 52), "matview", false))),
                        new Query(
                                new NodeLocation(3, 5),
                                Optional.of(new With(
                                        new NodeLocation(3, 5),
                                        false,
                                        ImmutableList.of(
                                                new WithQuery(
                                                        new NodeLocation(3, 10),
                                                        new Identifier(new NodeLocation(3, 10), "a", false),
                                                        new Query(
                                                                new NodeLocation(3, 23),
                                                                Optional.empty(),
                                                                new QuerySpecification(
                                                                        new NodeLocation(3, 23),
                                                                        new Select(
                                                                                new NodeLocation(3, 23),
                                                                                false,
                                                                                ImmutableList.of(new AllColumns(new NodeLocation(3, 30), Optional.empty(), ImmutableList.of()))),
                                                                        Optional.of(new Table(
                                                                                new NodeLocation(3, 37),
                                                                                QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(3, 37), "x", false))))),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        ImmutableList.of(),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        Optional.empty()),
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                Optional.empty()),
                                                        Optional.of(ImmutableList.of(
                                                                new Identifier(new NodeLocation(3, 13), "t", false),
                                                                new Identifier(new NodeLocation(3, 16), "u", false)))),
                                                new WithQuery(
                                                        new NodeLocation(3, 41),
                                                        new Identifier(new NodeLocation(3, 41), "b", false),
                                                        new Query(
                                                                new NodeLocation(3, 47),
                                                                Optional.empty(),
                                                                new QuerySpecification(
                                                                        new NodeLocation(3, 47),
                                                                        new Select(
                                                                                new NodeLocation(3, 47),
                                                                                false,
                                                                                ImmutableList.of(new AllColumns(new NodeLocation(3, 54), Optional.empty(), ImmutableList.of()))),
                                                                        Optional.of(new Table(
                                                                                new NodeLocation(3, 61),
                                                                                QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(3, 61), "a", false))))),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        ImmutableList.of(),
                                                                        Optional.empty(),
                                                                        Optional.empty(),
                                                                        Optional.empty()),
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                Optional.empty()),
                                                        Optional.empty())))),
                                new Table(
                                        new NodeLocation(3, 64),
                                        QualifiedName.of(ImmutableList.of(new Identifier(new NodeLocation(3, 70), "b", false)))),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        true,
                        false,
                        Optional.empty(),
                        ImmutableList.of(new Property(
                                new NodeLocation(2, 7),
                                new Identifier(new NodeLocation(2, 7), "partitioned_by", false),
                                new Array(
                                        new NodeLocation(2, 24),
                                        ImmutableList.of(new StringLiteral(new NodeLocation(2, 31), "dateint"))))),
                        Optional.of("A partitioned materialized view")));
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertStatement("REFRESH MATERIALIZED VIEW test",
                new RefreshMaterializedView(Optional.empty(), new Table(QualifiedName.of("test"))));

        assertStatement("REFRESH MATERIALIZED VIEW \"some name that contains space\"",
                new RefreshMaterializedView(Optional.empty(), new Table(QualifiedName.of("some name that contains space"))));
    }

    @Test
    public void testDropMaterializedView()
    {
        assertStatement("DROP MATERIALIZED VIEW a", new DropMaterializedView(QualifiedName.of("a"), false));
        assertStatement("DROP MATERIALIZED VIEW a.b", new DropMaterializedView(QualifiedName.of("a", "b"), false));
        assertStatement("DROP MATERIALIZED VIEW a.b.c", new DropMaterializedView(QualifiedName.of("a", "b", "c"), false));

        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a", new DropMaterializedView(QualifiedName.of("a"), true));
        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a.b", new DropMaterializedView(QualifiedName.of("a", "b"), true));
        assertStatement("DROP MATERIALIZED VIEW IF EXISTS a.b.c", new DropMaterializedView(QualifiedName.of("a", "b", "c"), true));
    }

    @Test
    public void testRenameMaterializedView()
    {
        assertStatement("ALTER MATERIALIZED VIEW a RENAME TO b", new RenameMaterializedView(QualifiedName.of("a"), QualifiedName.of("b"), false));
        assertStatement("ALTER MATERIALIZED VIEW IF EXISTS a RENAME TO b", new RenameMaterializedView(QualifiedName.of("a"), QualifiedName.of("b"), true));
    }

    @Test
    public void testSetMaterializedViewProperties()
    {
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES foo='bar'",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(new Property(new Identifier("foo"), new StringLiteral("bar")))));
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES foo=true",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(new Property(new Identifier("foo"), new BooleanLiteral("true")))));
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES foo=123",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(new Property(new Identifier("foo"), new LongLiteral("123")))));
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES foo=123, bar=456",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(
                                new Property(new Identifier("foo"), new LongLiteral("123")),
                                new Property(new Identifier("bar"), new LongLiteral("456")))));
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES \" s p a c e \"='bar'",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(new Property(new Identifier(" s p a c e "), new StringLiteral("bar")))));
        assertStatement(
                "ALTER MATERIALIZED VIEW a SET PROPERTIES foo=123, bar=DEFAULT",
                new SetProperties(
                        MATERIALIZED_VIEW,
                        QualifiedName.of("a"),
                        ImmutableList.of(
                                new Property(new Identifier("foo"), new LongLiteral("123")),
                                new Property(new Identifier("bar")))));

        assertStatementIsInvalid("ALTER MATERIALIZED VIEW a SET PROPERTIES")
                .withMessage("line 1:41: mismatched input '<EOF>'. Expecting: <identifier>");
        assertStatementIsInvalid("ALTER MATERIALIZED VIEW a SET PROPERTIES ()")
                .withMessage("line 1:42: mismatched input '('. Expecting: <identifier>");
        assertStatementIsInvalid("ALTER MATERIALIZED VIEW a SET PROPERTIES (foo='bar')")
                .withMessage("line 1:42: mismatched input '('. Expecting: <identifier>");
    }

    @Test
    public void testNullTreatment()
    {
        assertExpression("lead(x, 1) ignore nulls over()",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("lead"),
                        Optional.of(new WindowSpecification(Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.of(NullTreatment.IGNORE),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
        assertExpression("lead(x, 1) respect nulls over()",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("lead"),
                        Optional.of(new WindowSpecification(Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.of(NullTreatment.RESPECT),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
    }

    @Test
    public void testProcessingMode()
    {
        assertExpression("RUNNING LAST(x, 1)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LAST"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.of(new ProcessingMode(Optional.empty(), RUNNING)),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
        assertExpression("FINAL FIRST(x, 1)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("FIRST"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.of(new ProcessingMode(Optional.empty(), FINAL)),
                        ImmutableList.of(new Identifier("x"), new LongLiteral("1"))));
    }

    @Test
    public void testWindowSpecification()
    {
        assertExpression("rank() OVER someWindow",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowReference(new Identifier("someWindow"))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));

        assertExpression("rank() OVER (someWindow PARTITION BY x ORDER BY y ROWS CURRENT ROW)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowSpecification(
                                Optional.of(new Identifier("someWindow")),
                                ImmutableList.of(new Identifier("x")),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("y"), ASCENDING, UNDEFINED)))),
                                Optional.of(new WindowFrame(ROWS, new FrameBound(CURRENT_ROW), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of())))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));

        assertExpression("rank() OVER (PARTITION BY x ORDER BY y ROWS CURRENT ROW)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("rank"),
                        Optional.of(new WindowSpecification(
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x")),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("y"), ASCENDING, UNDEFINED)))),
                                Optional.of(new WindowFrame(ROWS, new FrameBound(CURRENT_ROW), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of())))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));
    }

    @Test
    public void testWindowClause()
    {
        assertStatement("SELECT * FROM T WINDOW someWindow AS (PARTITION BY a), otherWindow AS (someWindow ORDER BY b)",
                simpleQuery(
                        selectList(new AllColumns()),
                        new Table(makeQualifiedName("T")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                new WindowDefinition(
                                        new Identifier("someWindow"),
                                        new WindowSpecification(
                                                Optional.empty(),
                                                ImmutableList.of(new Identifier("a")),
                                                Optional.empty(),
                                                Optional.empty())),
                                new WindowDefinition(
                                        new Identifier("otherWindow"),
                                        new WindowSpecification(
                                                Optional.of(new Identifier("someWindow")),
                                                ImmutableList.of(),
                                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("b"), ASCENDING, UNDEFINED)))),
                                                Optional.empty()))),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Test
    public void testWindowFrameWithPatternRecognition()
    {
        assertThat(expression("""
                rank() OVER (
                   PARTITION BY x
                   ORDER BY y
                   MEASURES
                       MATCH_NUMBER() AS match_no,
                       LAST(A.z) AS last_z
                   ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING
                   AFTER MATCH SKIP TO NEXT ROW
                   SEEK
                   PATTERN (A B C)
                   SUBSET U = (A, B)
                   DEFINE
                       B AS false,
                       C AS CLASSIFIER(U) = 'B'
                 )
                """))
                .isEqualTo(new FunctionCall(
                        Optional.of(location(1, 1)),
                        QualifiedName.of(ImmutableList.of(new Identifier(location(1, 1), "rank", false))),
                        Optional.of(new WindowSpecification(
                                location(2, 4),
                                Optional.empty(),
                                ImmutableList.of(new Identifier(location(2, 17), "x", false)),
                                Optional.of(new OrderBy(
                                        location(3, 4),
                                        ImmutableList.of(new SortItem(location(3, 13), new Identifier(location(3, 13), "y", false), ASCENDING, UNDEFINED)))),
                                Optional.of(new WindowFrame(
                                        location(4, 4),
                                        ROWS,
                                        new FrameBound(location(7, 17), CURRENT_ROW),
                                        Optional.of(new FrameBound(location(7, 33), FOLLOWING, new LongLiteral(location(7, 33), "5"))),
                                        ImmutableList.of(
                                                new MeasureDefinition(
                                                        location(5, 8),
                                                        new FunctionCall(
                                                                location(5, 8),
                                                                QualifiedName.of(ImmutableList.of(new Identifier(location(5, 8), "MATCH_NUMBER", false))),
                                                                ImmutableList.of()),
                                                        new Identifier(location(5, 26), "match_no", false)),
                                                new MeasureDefinition(
                                                        location(6, 8),
                                                        new FunctionCall(
                                                                location(6, 8),
                                                                QualifiedName.of(ImmutableList.of(new Identifier(location(6, 8), "LAST", false))),
                                                                ImmutableList.of(new DereferenceExpression(
                                                                        location(6, 13),
                                                                        new Identifier(location(6, 13), "A", false),
                                                                        new Identifier(location(6, 15), "z", false)))),
                                                        new Identifier(location(6, 21), "last_z", false))),
                                        Optional.of(skipToNextRow(location(8, 16))),
                                        Optional.of(new PatternSearchMode(location(9, 4), SEEK)),
                                        Optional.of(new PatternConcatenation(
                                                location(10, 13),
                                                ImmutableList.of(
                                                        new PatternConcatenation(
                                                                location(10, 13),
                                                                ImmutableList.of(
                                                                        new PatternVariable(location(10, 13), new Identifier(location(10, 13), "A", false)),
                                                                        new PatternVariable(location(10, 15), new Identifier(location(10, 15), "B", false)))),
                                                        new PatternVariable(location(10, 17), new Identifier(location(10, 17), "C", false))))),
                                        ImmutableList.of(new SubsetDefinition(
                                                location(11, 11),
                                                new Identifier(location(11, 11), "U", false),
                                                ImmutableList.of(new Identifier(location(11, 16), "A", false), new Identifier(location(11, 19), "B", false)))),
                                        ImmutableList.of(
                                                new VariableDefinition(
                                                        location(13, 8),
                                                        new Identifier(location(13, 8), "B", false),
                                                        new BooleanLiteral(location(13, 13), "false")),
                                                new VariableDefinition(
                                                        location(14, 8),
                                                        new Identifier(location(14, 8), "C", false),
                                                        new ComparisonExpression(
                                                                location(14, 27),
                                                                EQUAL,
                                                                new FunctionCall(
                                                                        location(14, 13),
                                                                        QualifiedName.of(ImmutableList.of(new Identifier(location(14, 13), "CLASSIFIER", false))),
                                                                        ImmutableList.of(new Identifier(location(14, 24), "U", false))),
                                                                new StringLiteral(location(14, 29), "B")))))))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()));
    }

    @Test
    public void testMeasureOverWindow()
    {
        assertThat(expression("""
                last_z OVER (
                  MEASURES z AS last_z
                  ROWS CURRENT ROW
                  PATTERN (A)
                  DEFINE a AS true
                )
                """))
                .isEqualTo(new WindowOperation(
                        location(1, 1),
                        new Identifier(location(1, 1), "last_z", false),
                        new WindowSpecification(
                                location(2, 3),
                                Optional.empty(),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.of(new WindowFrame(
                                        location(2, 3),
                                        ROWS,
                                        new FrameBound(location(3, 8), CURRENT_ROW),
                                        Optional.empty(),
                                        ImmutableList.of(new MeasureDefinition(
                                                location(2, 12),
                                                new Identifier(location(2, 12), "z", false),
                                                new Identifier(location(2, 17), "last_z", false))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.of(new PatternVariable(location(4, 12), new Identifier(location(4, 12), "A", false))),
                                        ImmutableList.of(),
                                        ImmutableList.of(new VariableDefinition(
                                                location(5, 10),
                                                new Identifier(location(5, 10), "a", false),
                                                new BooleanLiteral(location(5, 15), "true"))))))));
    }

    @Test
    public void testAllRowsReference()
    {
        assertThatThrownBy(() -> SQL_PARSER.createStatement("SELECT 1 + A.*", new ParsingOptions(REJECT)))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:13: mismatched input '.'.*");

        assertThat(statement("SELECT A.*"))
                .ignoringLocation()
                .isEqualTo(simpleQuery(new Select(false, ImmutableList.of(new AllColumns(new Identifier("A"), ImmutableList.of())))));
    }

    @Test
    public void testUpdate()
    {
        assertStatement("""
                        UPDATE foo_table
                            SET bar = 23, baz = 3.1415E0, bletch = 'barf'
                        WHERE (nothing = 'fun')
                        """,
                new Update(
                        new NodeLocation(1, 1),
                        table(QualifiedName.of("foo_table")),
                        ImmutableList.of(
                                new UpdateAssignment(new Identifier("bar"), new LongLiteral("23")),
                                new UpdateAssignment(new Identifier("baz"), new DoubleLiteral("3.1415")),
                                new UpdateAssignment(new Identifier("bletch"), new StringLiteral("barf"))),
                        Optional.of(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new Identifier("nothing"), new StringLiteral("fun")))));
    }

    @Test
    public void testWherelessUpdate()
    {
        assertStatement("""
                        UPDATE foo_table
                        SET bar = 23
                        """,
                new Update(
                        new NodeLocation(1, 1),
                        table(QualifiedName.of("foo_table")),
                        ImmutableList.of(
                                new UpdateAssignment(new Identifier("bar"), new LongLiteral("23"))),
                        Optional.empty()));
    }

    @Test
    public void testQueryPeriod()
    {
        Expression rangeValue = new TimestampLiteral(location(1, 37), "2021-03-01 00:00:01");
        QueryPeriod queryPeriod = new QueryPeriod(location(1, 17), QueryPeriod.RangeType.TIMESTAMP, rangeValue);
        Table table = new Table(location(1, 15), qualifiedName(location(1, 15), "t"), queryPeriod);
        assertThat(statement("SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2021-03-01 00:00:01'"))
                .isEqualTo(
                        new Query(
                                location(1, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(1, 1),
                                        new Select(
                                                location(1, 1),
                                                false,
                                                ImmutableList.of(
                                                        new AllColumns(
                                                                location(1, 8),
                                                                Optional.empty(),
                                                                ImmutableList.of()))),
                                        Optional.of(table),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()));

        rangeValue = new StringLiteral(location(1, 35), "version1");
        queryPeriod = new QueryPeriod(new NodeLocation(1, 17), QueryPeriod.RangeType.VERSION, rangeValue);
        table = new Table(location(1, 15), qualifiedName(location(1, 15), "t"), queryPeriod);
        assertThat(statement("SELECT * FROM t FOR VERSION AS OF 'version1'"))
                .isEqualTo(
                        new Query(
                                location(1, 1),
                                Optional.empty(),
                                new QuerySpecification(
                                        location(1, 1),
                                        new Select(
                                                location(1, 1),
                                                false,
                                                ImmutableList.of(
                                                        new AllColumns(
                                                                location(1, 8),
                                                                Optional.empty(),
                                                                ImmutableList.of()))),
                                        Optional.of(table),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()));
    }

    @Test
    public void testListagg()
    {
        assertExpression("LISTAGG(x) WITHIN GROUP (ORDER BY x)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x", false), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(""),
                                new BooleanLiteral("true"),
                                new StringLiteral("..."),
                                new BooleanLiteral("false"))));

        assertExpression("LISTAGG( DISTINCT x) WITHIN GROUP (ORDER BY x)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x", false), ASCENDING, UNDEFINED)))),
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(""),
                                new BooleanLiteral("true"),
                                new StringLiteral("..."),
                                new BooleanLiteral("false"))));

        assertExpression("LISTAGG(x, ',') WITHIN GROUP (ORDER BY y)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("y", false), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(","),
                                new BooleanLiteral("true"),
                                new StringLiteral("..."),
                                new BooleanLiteral("false"))));

        assertExpression("LISTAGG(x, ',' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY x)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x", false), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(","),
                                new BooleanLiteral("true"),
                                new StringLiteral("..."),
                                new BooleanLiteral("false"))));

        assertExpression("LISTAGG(x, ',' ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY x)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x", false), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(","),
                                new BooleanLiteral("false"),
                                new StringLiteral("..."),
                                new BooleanLiteral("true"))));

        assertExpression("LISTAGG(x, ',' ON OVERFLOW TRUNCATE 'HIDDEN' WITHOUT COUNT) WITHIN GROUP (ORDER BY x)",
                new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of("LISTAGG"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x", false), ASCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(
                                identifier("x"),
                                new StringLiteral(","),
                                new BooleanLiteral("false"),
                                new StringLiteral("HIDDEN"),
                                new BooleanLiteral("false"))));
    }

    @Test
    public void testTableFunctionInvocation()
    {
        assertThat(statement("SELECT * FROM TABLE(some_ptf(input => 1))"))
                .isEqualTo(selectAllFrom(new TableFunctionInvocation(
                        location(1, 21),
                        qualifiedName(location(1, 21), "some_ptf"),
                        ImmutableList.of(new TableFunctionArgument(
                                location(1, 30),
                                Optional.of(new Identifier(location(1, 30), "input", false)),
                                new LongLiteral(location(1, 39), "1"))),
                        ImmutableList.of())));

        assertThat(statement("""
                SELECT * FROM TABLE(some_ptf(
                    arg1 => TABLE(orders) AS ord(a, b, c)
                        PARTITION BY a
                        PRUNE WHEN EMPTY
                        ORDER BY b ASC NULLS LAST,
                    arg2 => CAST(NULL AS DESCRIPTOR),
                    arg3 => DESCRIPTOR(x integer, y varchar),
                    arg4 => 5,
                    'not-named argument'
                    COPARTITION (ord, nation)))
                """))
                .isEqualTo(selectAllFrom(new TableFunctionInvocation(
                        location(1, 21),
                        qualifiedName(location(1, 21), "some_ptf"),
                        ImmutableList.of(
                                new TableFunctionArgument(
                                        location(2, 5),
                                        Optional.of(new Identifier(location(2, 5), "arg1", false)),
                                        new TableFunctionTableArgument(
                                                location(2, 13),
                                                new AliasedRelation(
                                                        location(2, 13),
                                                        new Table(location(2, 13), qualifiedName(location(2, 19), "orders")),
                                                        new Identifier(location(2, 30), "ord", false),
                                                        ImmutableList.of(
                                                                new Identifier(location(2, 34), "a", false),
                                                                new Identifier(location(2, 37), "b", false),
                                                                new Identifier(location(2, 40), "c", false))),
                                                Optional.of(ImmutableList.of(new Identifier(location(3, 22), "a", false))),
                                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(location(5, 18), new Identifier(location(5, 18), "b", false), ASCENDING, LAST)))),
                                                Optional.of(new EmptyTableTreatment(location(4, 9), PRUNE)))),
                                new TableFunctionArgument(
                                        location(6, 5),
                                        Optional.of(new Identifier(location(6, 5), "arg2", false)),
                                        nullDescriptorArgument(location(6, 13))),
                                new TableFunctionArgument(
                                        location(7, 5),
                                        Optional.of(new Identifier(location(7, 5), "arg3", false)),
                                        descriptorArgument(
                                                location(7, 13),
                                                new Descriptor(location(7, 13), ImmutableList.of(
                                                        new DescriptorField(
                                                                location(7, 24),
                                                                new Identifier(location(7, 24), "x", false),
                                                                Optional.of(new GenericDataType(location(7, 26), new Identifier(location(7, 26), "integer", false), ImmutableList.of()))),
                                                        new DescriptorField(
                                                                location(7, 35),
                                                                new Identifier(location(7, 35), "y", false),
                                                                Optional.of(new GenericDataType(location(7, 37), new Identifier(location(7, 37), "varchar", false), ImmutableList.of()))))))),
                                new TableFunctionArgument(
                                        location(8, 5),
                                        Optional.of(new Identifier(location(8, 5), "arg4", false)),
                                        new LongLiteral(location(8, 13), "5")),
                                new TableFunctionArgument(
                                        location(9, 5),
                                        Optional.empty(),
                                        new StringLiteral(location(9, 5), "not-named argument"))),
                        ImmutableList.of(ImmutableList.of(
                                qualifiedName(location(10, 18), "ord"),
                                qualifiedName(location(10, 23), "nation"))))));
    }

    @Test
    public void testTableFunctionTableArgumentAliasing()
    {
        // no alias
        assertThat(statement("SELECT * FROM TABLE(some_ptf(input => TABLE(orders)))"))
                .isEqualTo(selectAllFrom(new TableFunctionInvocation(
                        location(1, 21),
                        qualifiedName(location(1, 21), "some_ptf"),
                        ImmutableList.of(new TableFunctionArgument(
                                location(1, 30),
                                Optional.of(new Identifier(location(1, 30), "input", false)),
                                new TableFunctionTableArgument(
                                        location(1, 39),
                                        new Table(location(1, 39), qualifiedName(location(1, 45), "orders")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                        ImmutableList.of())));

        // table alias; no column aliases
        assertThat(statement("SELECT * FROM TABLE(some_ptf(input => TABLE(orders) AS ord))"))
                .isEqualTo(selectAllFrom(new TableFunctionInvocation(
                        location(1, 21),
                        qualifiedName(location(1, 21), "some_ptf"),
                        ImmutableList.of(new TableFunctionArgument(
                                location(1, 30),
                                Optional.of(new Identifier(location(1, 30), "input", false)),
                                new TableFunctionTableArgument(
                                        location(1, 39),
                                        new AliasedRelation(
                                                location(1, 39),
                                                new Table(location(1, 39), qualifiedName(location(1, 45), "orders")),
                                                new Identifier(location(1, 56), "ord", false),
                                                null),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                        ImmutableList.of())));

        // table alias and column aliases
        assertThat(statement("SELECT * FROM TABLE(some_ptf(input => TABLE(orders) AS ord(a, b, c)))"))
                .isEqualTo(selectAllFrom(new TableFunctionInvocation(
                        location(1, 21),
                        qualifiedName(location(1, 21), "some_ptf"),
                        ImmutableList.of(new TableFunctionArgument(
                                location(1, 30),
                                Optional.of(new Identifier(location(1, 30), "input", false)),
                                new TableFunctionTableArgument(
                                        location(1, 39),
                                        new AliasedRelation(
                                                location(1, 39),
                                                new Table(location(1, 39), qualifiedName(location(1, 45), "orders")),
                                                new Identifier(location(1, 56), "ord", false),
                                                ImmutableList.of(
                                                        new Identifier(location(1, 60), "a", false),
                                                        new Identifier(location(1, 63), "b", false),
                                                        new Identifier(location(1, 66), "c", false))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                        ImmutableList.of())));
    }

    @Test
    public void testCopartitionInTableArgumentAlias()
    {
        // table argument 'input' is aliased. The alias "copartition" is illegal in this context.
        assertThatThrownBy(() -> SQL_PARSER.createStatement("""
                        SELECT *
                        FROM TABLE(some_ptf(
                        input => TABLE(orders) copartition(a, b, c)))
                        """,
                new ParsingOptions()))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 3:24: The word \"COPARTITION\" is ambiguous in this context. " +
                        "To alias an argument, precede the alias with \"AS\". " +
                        "To specify co-partitioning, change the argument order so that the last argument cannot be aliased.");

        // table argument 'input' contains an aliased relation with the alias "copartition". The alias is enclosed in the 'TABLE(...)' clause, and the argument itself is not aliased.
        // The alias "copartition" is legal in this context.
        assertThat(statement("""
                SELECT *
                FROM TABLE(some_ptf(
                   input => TABLE(SELECT * FROM orders copartition(a, b, c))))
                """))
                .isInstanceOf(Query.class);

        // table argument 'input' is aliased. The alias "COPARTITION" is delimited, so it can cause no ambiguity with the COPARTITION clause, and is considered legal in this context.
        assertThat(statement("""
                SELECT *
                FROM TABLE(some_ptf(
                    input => TABLE(orders) "COPARTITION"(a, b, c)))
                """))
                .isInstanceOf(Query.class);

        // table argument 'input' is aliased. The alias "copartition" is preceded with the keyword "AS", so it can cause no ambiguity with the COPARTITION clause, and is considered legal in this context.
        assertThat(statement("""
                SELECT *
                FROM TABLE(some_ptf(
                   input => TABLE(orders) AS copartition(a, b, c)))
                """))
                .isInstanceOf(Query.class);

        // the COPARTITION word can be either the alias for argument 'input3', or part of the COPARTITION clause.
        // It is parsed as the argument alias, and then fails as illegal in this context.
        assertThatThrownBy(() -> SQL_PARSER.createStatement("""
                SELECT *
                FROM TABLE(some_ptf(
                    input1 => TABLE(customers) PARTITION BY nationkey,
                    input2 => TABLE(nation) PARTITION BY nationkey,
                    input3 => TABLE(lineitem)
                    COPARTITION(customers, nation)))
                """,
                new ParsingOptions()))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 6:5: The word \"COPARTITION\" is ambiguous in this context. " +
                        "To alias an argument, precede the alias with \"AS\". " +
                        "To specify co-partitioning, change the argument order so that the last argument cannot be aliased.");

        // the above query does not fail if we change the order of arguments so that the last argument before the COPARTITION clause has specified partitioning.
        // In such case, the COPARTITION word cannot be mistaken for alias.
        // Note that this transformation of the query is always available. If the table function invocation contains the COPARTITION clause,
        // at least two table arguments must have partitioning specified.
        assertThat(statement("""
                SELECT *
                FROM TABLE(some_ptf(
                    input1 => TABLE(customers) PARTITION BY nationkey,
                    input3 => TABLE(lineitem),
                    input2 => TABLE(nation) PARTITION BY nationkey
                    COPARTITION(customers, nation)))
                """))
                .isInstanceOf(Query.class);
    }

    private static Query selectAllFrom(Relation relation)
    {
        return new Query(
                location(1, 1),
                Optional.empty(),
                new QuerySpecification(
                        location(1, 1),
                        new Select(location(1, 1), false, ImmutableList.of(new AllColumns(location(1, 8), Optional.empty(), ImmutableList.of()))),
                        Optional.of(relation),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testJsonExists()
    {
        // test defaults
        assertThat(expression("JSON_EXISTS(json_column, 'lax $[5]')"))
                .isEqualTo(new JsonExists(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(1, 13)),
                                new Identifier(location(1, 13), "json_column", false),
                                JSON,
                                new StringLiteral(location(1, 26), "lax $[5]"),
                                Optional.empty(),
                                ImmutableList.of()),
                        JsonExists.ErrorBehavior.FALSE));

        assertThat(expression("""
                JSON_EXISTS(
                    json_column FORMAT JSON ENCODING UTF8,
                    'lax $[start_parameter TO end_parameter.ceiling()]'
                        PASSING
                                start_column AS start_parameter,
                                end_column FORMAT JSON ENCODING UTF16 AS end_parameter
                    UNKNOWN ON ERROR)
                """))
                .isEqualTo(new JsonExists(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(2, 5)),
                                new Identifier(location(2, 5), "json_column", false),
                                UTF8,
                                new StringLiteral(location(3, 5), "lax $[start_parameter TO end_parameter.ceiling()]"),
                                Optional.empty(),
                                ImmutableList.of(
                                        new JsonPathParameter(
                                                Optional.of(location(5, 17)),
                                                new Identifier(location(5, 33), "start_parameter", false),
                                                new Identifier(location(5, 17), "start_column", false),
                                                Optional.empty()),
                                        new JsonPathParameter(
                                                Optional.of(location(6, 17)),
                                                new Identifier(location(6, 58), "end_parameter", false),
                                                new Identifier(location(6, 17), "end_column", false),
                                                Optional.of(UTF16)))),
                        JsonExists.ErrorBehavior.UNKNOWN));
    }

    @Test
    public void testJsonValue()
    {
        // test defaults
        assertThat(expression("JSON_VALUE(json_column, 'lax $[5]')"))
                .isEqualTo(new JsonValue(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(1, 12)),
                                new Identifier(location(1, 12), "json_column", false),
                                JSON,
                                new StringLiteral(location(1, 25), "lax $[5]"),
                                Optional.empty(),
                                ImmutableList.of()),
                        Optional.empty(),
                        JsonValue.EmptyOrErrorBehavior.NULL,
                        Optional.empty(),
                        JsonValue.EmptyOrErrorBehavior.NULL,
                        Optional.empty()));

        assertThat(expression("""
                JSON_VALUE(
                    json_column FORMAT JSON ENCODING UTF8,
                    'lax $[start_parameter TO end_parameter.ceiling()]'
                        PASSING
                                start_column AS start_parameter,
                                end_column FORMAT JSON ENCODING UTF16 AS end_parameter
                    RETURNING double
                    DEFAULT 5e0 ON EMPTY
                    ERROR ON ERROR)
                """))
                .isEqualTo(new JsonValue(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(2, 5)),
                                new Identifier(location(2, 5), "json_column", false),
                                UTF8,
                                new StringLiteral(location(3, 5), "lax $[start_parameter TO end_parameter.ceiling()]"),
                                Optional.empty(),
                                ImmutableList.of(
                                        new JsonPathParameter(
                                                Optional.of(location(5, 17)),
                                                new Identifier(location(5, 33), "start_parameter", false),
                                                new Identifier(location(5, 17), "start_column", false),
                                                Optional.empty()),
                                        new JsonPathParameter(
                                                Optional.of(location(6, 17)),
                                                new Identifier(location(6, 58), "end_parameter", false),
                                                new Identifier(location(6, 17), "end_column", false),
                                                Optional.of(UTF16)))),
                        Optional.of(new GenericDataType(location(7, 15), new Identifier(location(7, 15), "double", false), ImmutableList.of())),
                        JsonValue.EmptyOrErrorBehavior.DEFAULT,
                        Optional.of(new DoubleLiteral(location(8, 13), "5e0")),
                        JsonValue.EmptyOrErrorBehavior.ERROR,
                        Optional.empty()));
    }

    @Test
    public void testJsonQuery()
    {
        // test defaults
        assertThat(expression("JSON_QUERY(json_column, 'lax $[5]')"))
                .isEqualTo(new JsonQuery(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(1, 12)),
                                new Identifier(location(1, 12), "json_column", false),
                                JSON,
                                new StringLiteral(location(1, 25), "lax $[5]"),
                                Optional.empty(),
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        JsonQuery.ArrayWrapperBehavior.WITHOUT,
                        Optional.empty(),
                        JsonQuery.EmptyOrErrorBehavior.NULL,
                        JsonQuery.EmptyOrErrorBehavior.NULL));

        assertThat(expression("""
                JSON_QUERY(
                    json_column FORMAT JSON ENCODING UTF8,
                    'lax $[start_parameter TO end_parameter.ceiling()]'
                        PASSING
                                start_column AS start_parameter,
                                end_column FORMAT JSON ENCODING UTF16 AS end_parameter
                    RETURNING varchar FORMAT JSON ENCODING UTF32
                    WITH ARRAY WRAPPER
                    OMIT QUOTES
                    EMPTY ARRAY ON EMPTY
                    ERROR ON ERROR)
                """))
                .isEqualTo(new JsonQuery(
                        Optional.of(location(1, 1)),
                        new JsonPathInvocation(
                                Optional.of(location(2, 5)),
                                new Identifier(location(2, 5), "json_column", false),
                                UTF8,
                                new StringLiteral(location(3, 5), "lax $[start_parameter TO end_parameter.ceiling()]"),
                                Optional.empty(),
                                ImmutableList.of(
                                        new JsonPathParameter(
                                                Optional.of(location(5, 17)),
                                                new Identifier(location(5, 33), "start_parameter", false),
                                                new Identifier(location(5, 17), "start_column", false),
                                                Optional.empty()),
                                        new JsonPathParameter(
                                                Optional.of(location(6, 17)),
                                                new Identifier(location(6, 58), "end_parameter", false),
                                                new Identifier(location(6, 17), "end_column", false),
                                                Optional.of(UTF16)))),
                        Optional.of(new GenericDataType(location(7, 15), new Identifier(location(7, 15), "varchar", false), ImmutableList.of())),
                        Optional.of(UTF32),
                        JsonQuery.ArrayWrapperBehavior.UNCONDITIONAL,
                        Optional.of(JsonQuery.QuotesBehavior.OMIT),
                        JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY,
                        JsonQuery.EmptyOrErrorBehavior.ERROR));
    }

    @Test
    public void testJsonObject()
    {
        // test create empty JSON object
        assertThat(expression("JSON_OBJECT()"))
                .isEqualTo(new JsonObject(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(),
                        true,
                        false,
                        Optional.empty(),
                        Optional.empty()));

        // test defaults
        assertThat(expression("JSON_OBJECT(key_column : value_column)"))
                .isEqualTo(new JsonObject(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(new JsonObjectMember(
                                location(1, 13),
                                new Identifier(location(1, 13), "key_column", false),
                                new Identifier(location(1, 26), "value_column", false),
                                Optional.empty())),
                        true,
                        false,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(expression("""
                JSON_OBJECT(
                     key_column_1 VALUE value_column FORMAT JSON ENCODING UTF16,
                     KEY 'key_literal' VALUE 5,
                     key_column_2 : null
                     ABSENT ON NULL
                     WITH UNIQUE KEYS
                     RETURNING varbinary FORMAT JSON ENCODING UTF32)
                """))
                .isEqualTo(new JsonObject(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(
                                new JsonObjectMember(
                                        location(2, 6),
                                        new Identifier(location(2, 6), "key_column_1", false),
                                        new Identifier(location(2, 25), "value_column", false),
                                        Optional.of(UTF16)),
                                new JsonObjectMember(
                                        location(3, 6),
                                        new StringLiteral(location(3, 10), "key_literal"),
                                        new LongLiteral(location(3, 30), "5"),
                                        Optional.empty()),
                                new JsonObjectMember(
                                        location(4, 6),
                                        new Identifier(location(4, 6), "key_column_2", false),
                                        new NullLiteral(location(4, 21)),
                                        Optional.empty())),
                        false,
                        true,
                        Optional.of(new GenericDataType(location(7, 16), new Identifier(location(7, 16), "varbinary", false), ImmutableList.of())),
                        Optional.of(UTF32)));
    }

    @Test
    public void testJsonArray()
    {
        // test create empty JSON array
        assertThat(expression("JSON_ARRAY()"))
                .isEqualTo(new JsonArray(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty()));

        // test defaults
        assertThat(expression("JSON_ARRAY(value_column)"))
                .isEqualTo(new JsonArray(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(new JsonArrayElement(
                                location(1, 12),
                                new Identifier(location(1, 12), "value_column", false),
                                Optional.empty())),
                        false,
                        Optional.empty(),
                        Optional.empty()));

        assertThat(expression("""
                JSON_ARRAY(value_column FORMAT JSON ENCODING UTF16,
                    5,
                    null
                    NULL ON NULL
                    RETURNING varbinary FORMAT JSON ENCODING UTF32)
                """))
                .isEqualTo(new JsonArray(
                        Optional.of(location(1, 1)),
                        ImmutableList.of(
                                new JsonArrayElement(
                                        location(1, 12),
                                        new Identifier(location(1, 12), "value_column", false),
                                        Optional.of(UTF16)),
                                new JsonArrayElement(
                                        location(2, 5),
                                        new LongLiteral(location(2, 5), "5"),
                                        Optional.empty()),
                                new JsonArrayElement(
                                        location(3, 5),
                                        new NullLiteral(location(3, 5)),
                                        Optional.empty())),
                        true,
                        Optional.of(new GenericDataType(location(5, 15), new Identifier(location(5, 15), "varbinary", false), ImmutableList.of())),
                        Optional.of(UTF32)));
    }

    @Test
    public void testJsonTableScalarColumns()
    {
        // test json_table with ordinality column, value column, and query column
        assertThat(statement("SELECT * FROM JSON_TABLE(col, 'lax $' COLUMNS(" +
                "ordinal_number FOR ORDINALITY, " +
                "customer_name varchar PATH 'lax $.cust_no' DEFAULT 'anonymous' ON EMPTY null ON ERROR, " +
                "customer_countries varchar FORMAT JSON PATH 'lax.cust_ctr[*]' WITH WRAPPER KEEP QUOTES null ON EMPTY ERROR ON ERROR," +
                "customer_regions varchar FORMAT JSON PATH 'lax.cust_reg[*]' EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR) " +
                "EMPTY ON ERROR)"))
                .isEqualTo(selectAllFrom(new JsonTable(
                        location(1, 15),
                        new JsonPathInvocation(
                                Optional.of(location(1, 26)),
                                new Identifier(location(1, 26), "col", false),
                                JSON,
                                new StringLiteral(location(1, 31), "lax $"),
                                Optional.empty(),
                                ImmutableList.of()),
                        ImmutableList.of(
                                new OrdinalityColumn(location(1, 47), new Identifier(location(1, 47), "ordinal_number", false)),
                                new ValueColumn(
                                        location(1, 78),
                                        new Identifier(location(1, 78), "customer_name", false),
                                        new GenericDataType(location(1, 92), new Identifier(location(1, 92), "varchar", false), ImmutableList.of()),
                                        Optional.of(new StringLiteral(location(1, 105), "lax $.cust_no")),
                                        JsonValue.EmptyOrErrorBehavior.DEFAULT,
                                        Optional.of(new StringLiteral(location(1, 129), "anonymous")),
                                        Optional.of(JsonValue.EmptyOrErrorBehavior.NULL),
                                        Optional.empty()),
                                new QueryColumn(
                                        location(1, 165),
                                        new Identifier(location(1, 165), "customer_countries", false),
                                        new GenericDataType(location(1, 184), new Identifier(location(1, 184), "varchar", false), ImmutableList.of()),
                                        JSON,
                                        Optional.of(new StringLiteral(location(1, 209), "lax.cust_ctr[*]")),
                                        JsonQuery.ArrayWrapperBehavior.UNCONDITIONAL,
                                        Optional.of(JsonQuery.QuotesBehavior.KEEP),
                                        JsonQuery.EmptyOrErrorBehavior.NULL,
                                        Optional.of(JsonQuery.EmptyOrErrorBehavior.ERROR)),
                                new QueryColumn(
                                        location(1, 281),
                                        new Identifier(location(1, 281), "customer_regions", false),
                                        new GenericDataType(location(1, 298), new Identifier(location(1, 298), "varchar", false), ImmutableList.of()),
                                        JSON,
                                        Optional.of(new StringLiteral(location(1, 323), "lax.cust_reg[*]")),
                                        JsonQuery.ArrayWrapperBehavior.WITHOUT,
                                        Optional.empty(),
                                        JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY,
                                        Optional.of(JsonQuery.EmptyOrErrorBehavior.EMPTY_OBJECT))),
                        Optional.empty(),
                        Optional.of(JsonTable.ErrorBehavior.EMPTY))));
    }

    @Test
    public void testJsonTableNestedColumns()
    {
        // test json_table with nested columns and PLAN clause
        assertThat(statement("""
                SELECT * FROM JSON_TABLE(col, 'lax $' AS customer COLUMNS(
                    NESTED PATH 'lax $.cust_status[*]' AS status COLUMNS(
                       status varchar PATH 'lax $.type',
                       fresh boolean PATH 'lax &.new'),
                    NESTED PATH 'lax &.cust_comm[*]' AS comment COLUMNS(
                       comment varchar PATH 'lax $.text'))
                    PLAN (customer OUTER (status CROSS comment))
                    ERROR ON ERROR)
                """))
                .isEqualTo(selectAllFrom(new JsonTable(
                        location(1, 15),
                        new JsonPathInvocation(
                                Optional.of(location(1, 26)),
                                new Identifier(location(1, 26), "col", false),
                                JSON,
                                new StringLiteral(location(1, 31), "lax $"),
                                Optional.of(new Identifier(location(1, 42), "customer", false)),
                                ImmutableList.of()),
                        ImmutableList.of(
                                new NestedColumns(
                                        location(2, 5),
                                        new StringLiteral(location(2, 17), "lax $.cust_status[*]"),
                                        Optional.of(new Identifier(location(2, 43), "status", false)),
                                        ImmutableList.of(
                                                new ValueColumn(
                                                        location(3, 8),
                                                        new Identifier(location(3, 8), "status", false),
                                                        new GenericDataType(location(3, 15), new Identifier(location(3, 15), "varchar", false), ImmutableList.of()),
                                                        Optional.of(new StringLiteral(location(3, 28), "lax $.type")),
                                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty()),
                                                new ValueColumn(
                                                        location(4, 8),
                                                        new Identifier(location(4, 8), "fresh", false),
                                                        new GenericDataType(location(4, 14), new Identifier(location(4, 14), "boolean", false), ImmutableList.of()),
                                                        Optional.of(new StringLiteral(location(4, 27), "lax &.new")),
                                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty()))),
                                new NestedColumns(
                                        location(5, 5),
                                        new StringLiteral(location(5, 17), "lax &.cust_comm[*]"),
                                        Optional.of(new Identifier(location(5, 41), "comment", false)),
                                        ImmutableList.of(
                                                new ValueColumn(
                                                        location(6, 8),
                                                        new Identifier(location(6, 8), "comment", false),
                                                        new GenericDataType(location(6, 16), new Identifier(location(6, 16), "varchar", false), ImmutableList.of()),
                                                        Optional.of(new StringLiteral(location(6, 29), "lax $.text")),
                                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        Optional.empty())))),
                        Optional.of(new PlanParentChild(
                                location(7, 11),
                                JsonTablePlan.ParentChildPlanType.OUTER,
                                new PlanLeaf(location(7, 11), new Identifier(location(7, 11), "customer", false)),
                                new PlanSiblings(
                                        location(7, 27),
                                        JsonTablePlan.SiblingsPlanType.CROSS,
                                        ImmutableList.of(
                                                new PlanLeaf(location(7, 27), new Identifier(location(7, 27), "status", false)),
                                                new PlanLeaf(location(7, 40), new Identifier(location(7, 40), "comment", false)))))),
                        Optional.of(JsonTable.ErrorBehavior.ERROR))));
    }

    private static QualifiedName makeQualifiedName(String tableName)
    {
        List<Identifier> parts = Splitter.on('.').splitToList(tableName).stream()
                .map(Identifier::new)
                .collect(toImmutableList());
        return QualifiedName.of(parts);
    }

    /**
     * @deprecated use {@link ParserAssert#statement(String)} instead
     */
    @Deprecated
    private static void assertStatement(@Language("SQL") String query, Statement expected)
    {
        assertParsed(query, expected, SQL_PARSER.createStatement(query, new ParsingOptions()));
        assertFormattedSql(SQL_PARSER, expected);
    }

    /**
     * @deprecated use {@link ParserAssert#expression(String)} instead
     */
    @Deprecated
    private static void assertExpression(@Language("SQL") String expression, Expression expected)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(expected, "expected is null");
        assertParsed(expression, expected, SQL_PARSER.createExpression(expression, new ParsingOptions(AS_DECIMAL)));
    }

    private static void assertParsed(String input, Node expected, Node parsed)
    {
        if (!parsed.equals(expected)) {
            fail(format("expected\n\n%s\n\nto parse as\n\n%s\n\nbut was\n\n%s\n",
                    indent(input),
                    indent(formatSql(expected)),
                    indent(formatSql(parsed))));
        }
    }

    private static void assertInvalidExpression(String expression, String expectedErrorMessageRegex)
    {
        assertExpressionIsInvalid(expression)
                .withMessageMatching("line \\d+:\\d+: " + expectedErrorMessageRegex);
    }

    private static String indent(String value)
    {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }

    private static Expression createExpression(String expression)
    {
        return SQL_PARSER.createExpression(expression, new ParsingOptions());
    }
}
