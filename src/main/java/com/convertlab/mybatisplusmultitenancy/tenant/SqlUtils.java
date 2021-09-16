package com.convertlab.mybatisplusmultitenancy.tenant;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class SqlUtils {

    private static final DbType MYSQL_STRING = DbType.mysql;

    private final static com.google.common.cache.Cache<String, MySqlSelectQueryBlock> selectCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(1500, TimeUnit.SECONDS).build();
    private final static com.google.common.cache.Cache<String, String> unionSelectCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(1500, TimeUnit.SECONDS).build();

    public String parseSql(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        // 查询语句
        if (stmt instanceof SQLSelectStatement) {
            SQLSelect sqlSelect = ((SQLSelectStatement) stmt).getSelect();
            if (sqlSelect.getQuery() instanceof SQLUnionQuery) {
                SQLUnionQuery unionQuery = (SQLUnionQuery) sqlSelect.getQuery();
                sql = unionSelect(unionQuery);
            } else {
                sql = select((MySqlSelectQueryBlock) sqlSelect.getQueryBlock());
            }
        }
        // 更新语句
        if (stmt instanceof SQLUpdateStatement) {
            sql = doUpdateSql(this, stmt);
        }
        //删除语句
        if (stmt instanceof SQLDeleteStatement) {
            sql = doDeleteSql(stmt);
        }
        return sql;
    }


    /**
     * 处理更新语句
     *
     * @param sqlUtils
     * @param stmt     解析的语句
     * @return 修改的后的sql
     */
    private static String doUpdateSql(SqlUtils sqlUtils, SQLStatement stmt) {
        MySqlUpdateStatement update = (MySqlUpdateStatement) stmt;
        SQLExpr where = update.getWhere();
        sqlUtils.enhancedWhereSubQuery(where);
        final String name = update.getTableName().getSimpleName();
        final SourceFromInfo table = SourceFromInfo.builder().tableName(name).needAddCondition(true).build();
        final SQLBinaryOpExpr tenantIdCondition = SqlParseUtil.generateTenantIdCondition(table);
        where = SqlParseUtil.generateCondition(where, tenantIdCondition, SQLBinaryOperator.BooleanAnd);
        update.setWhere(where);
        return update.toString();
    }


    private String doDeleteSql(SQLStatement stmt) {
        MySqlDeleteStatement delete = (MySqlDeleteStatement) stmt;
        SQLExpr where = delete.getWhere();
        enhancedWhereSubQuery(where);
        final String name = delete.getTableName().getSimpleName();
        final String alias = delete.getAlias();
        final SourceFromInfo table = SourceFromInfo.builder().alias(alias).tableName(name).needAddCondition(true).build();
        final SQLBinaryOpExpr tenantIdCondition = SqlParseUtil.generateTenantIdCondition(table);
        where = SqlParseUtil.generateCondition(where, tenantIdCondition, SQLBinaryOperator.BooleanAnd);
        delete.setWhere(where);
        return delete.toString();
    }


    /**
     * 查询语句
     *
     * @return
     */
    private String select(MySqlSelectQueryBlock select) {
        List<String> tableList = new ArrayList<>();
        enhancedSelectList(select);
        enhancedFrom(select, tableList);
        enhancedWhere(select, tableList);
        return select.toString();
    }

    /**
     * 处理union查询语句
     *
     * @param unionQuery 语句
     * @return 处理结果
     */
    private String unionSelect(SQLUnionQuery unionQuery) {
        SQLSelectQuery left = unionQuery.getLeft();
        SQLSelectQuery right = unionQuery.getRight();
        if (left instanceof SQLUnionQuery) {
            unionSelect((SQLUnionQuery) left);
        } else {
            select((MySqlSelectQueryBlock) left);
        }
        if (right instanceof SQLUnionQuery) {
            unionSelect((SQLUnionQuery) right);
        } else {
            select((MySqlSelectQueryBlock) right);
        }
        return String.valueOf(unionQuery);
    }

    /**
     * 加强 查询子语句
     *
     * @param select
     */
    public void enhancedSelectList(MySqlSelectQueryBlock select) {
        List<SQLSelectItem> selectList = select.getSelectList();
        // 遍历查询的字段，如果查询字段中有子查询 则加上租户ID查询条件
        selectList.forEach(e -> {
            if (e.getExpr() instanceof SQLQueryExpr) {
                SQLQueryExpr expr = (SQLQueryExpr) e.getExpr();
                String newFieldSql = select((MySqlSelectQueryBlock) expr.getSubQuery().getQueryBlock());
                SQLExpr subSelect = SQLUtils.toMySqlExpr(newFieldSql);
                e.setExpr(subSelect);
            }
        });
    }


    /**
     * 加强 from 语句
     *
     * @param select
     * @param tableList
     */
    public void enhancedFrom(MySqlSelectQueryBlock select, List<String> tableList) {
        final SQLTableSource from = select.getFrom();
        // 关联子查询
        if (from instanceof SQLSubqueryTableSource) {
            doEnhancedFromSubQuery((SQLSubqueryTableSource) from);
        }
        // 多表 复杂关联
        if (from instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinFrom = (SQLJoinTableSource) from;
            doEnhancedFromJoin(joinFrom, tableList);
        }

    }

    /**
     * 增强on 语句
     *
     * @param joinFrom
     * @param tableList
     */
    private void doEnhancedFromOn(SQLJoinTableSource joinFrom, List<String> tableList) {
        final SQLExprTableSource fromRight = (SQLExprTableSource) joinFrom.getRight();
        final SourceFromInfo fromInfo = SourceFromInfo
                .builder()
                .tableName(fromRight.getTableName())
                .alias(fromRight.getAlias())
                .build();
        final SQLBinaryOpExpr condition = SqlParseUtil.generateTenantIdCondition(fromInfo);
        if (joinFrom.getCondition() != null) {
            joinFrom.addCondition(condition);
            tableList.add(SqlParseUtil.getTableName(fromRight));
        }
    }

    /**
     * 加强
     *
     */
    private void doEnhancedFromJoin(SQLTableSource joinFrom, List<String> tableList) {
        if (joinFrom != null) {
            if (joinFrom instanceof SQLSubqueryTableSource) {
                doEnhancedFromSubQuery((SQLSubqueryTableSource) joinFrom);
            }
            if (joinFrom instanceof SQLJoinTableSource) {
                doEnhancedFromJoin(((SQLJoinTableSource) joinFrom).getLeft(), tableList);
                doEnhancedFromJoin(((SQLJoinTableSource) joinFrom).getRight(), tableList);
                // 增强on 语句
                if (((SQLJoinTableSource) joinFrom).getRight() instanceof SQLExprTableSource) {
                    doEnhancedFromOn((SQLJoinTableSource) joinFrom, tableList);
                }
            }
        }
    }

    /**
     * 加强 from子语句
     *
     * @param from
     */
    private void doEnhancedFromSubQuery(SQLSubqueryTableSource from) {
        String subQuery = select((MySqlSelectQueryBlock) from.getSelect().getQueryBlock());
        SQLSelect sqlSelectBySql = getSqlSelectBySql(subQuery);
        from.setSelect(sqlSelectBySql);
    }

    public void enhancedWhere(MySqlSelectQueryBlock select, List<String> tableList) {
        final SQLExpr where = select.getWhere();
        enhancedWhereSubQuery(where);
        doEnhancedWhere(select, where, tableList);
    }

    private void doEnhancedWhere(MySqlSelectQueryBlock select, SQLExpr where, List<String> tableList) {
        final SQLTableSource from = select.getFrom();
        List<SourceFromInfo> tableNameList = new ArrayList<>();
        getTableNames(from, tableNameList);
        tableNameList = tableNameList.stream()
                .filter(next -> !tableList.contains(next.getAlias()) && !tableList.contains(next.getTableName()))
                .distinct()
                .collect(Collectors.toList());
        // 根据多个表名获取拼接条件
        SQLBinaryOpExpr conditionByTableName = generateWhereConditionByTableList(tableNameList);
        where = SqlParseUtil.generateCondition(where, conditionByTableName, SQLBinaryOperator.BooleanAnd);
        select.setWhere(where);
    }


    /**
     * 根据from语句得到的表名拼接条件
     *
     * @param tableNameList 表名列表
     * @return 拼接后的条件
     */
    private SQLBinaryOpExpr generateWhereConditionByTableList(List<SourceFromInfo> tableNameList) {
        if (CollectionUtils.isEmpty(tableNameList)) {
            return null;
        }
        SQLBinaryOpExpr allCondition = new SQLBinaryOpExpr(MYSQL_STRING);
        for (int i = 0; i < tableNameList.size(); i++) {
            SourceFromInfo tableNameInfo = tableNameList.get(i);
            SQLBinaryOpExpr thisTenantIdWhere = SqlParseUtil.generateTenantIdCondition(tableNameInfo);
            // 如果是最后一个且不是第一个则将当期table条件设置为右侧条件
            if (i > 0 && i == tableNameList.size() - 1) {
                allCondition.setOperator(SQLBinaryOperator.BooleanAnd);
                allCondition.setRight(thisTenantIdWhere);
                break;
            }
            // 如果是只有一个table 则直接设置最终条件为当期table条件
            if (tableNameList.size() == 1) {
                allCondition = thisTenantIdWhere;
                break;
            }
            if (allCondition.getLeft() == null) {
                allCondition.setLeft(thisTenantIdWhere);
            } else {
                SQLExpr condition = SqlParseUtil.generateCondition(allCondition.getLeft(), thisTenantIdWhere, SQLBinaryOperator.BooleanAnd);
                allCondition.setLeft(condition);
            }
        }
        return allCondition;
    }


    /**
     * 查询所有的表信息
     *
     * @param tableSource   from语句
     * @param tableNameList sql中from语句中所有表信息
     */
    private void getTableNames(SQLTableSource tableSource,
                               List<SourceFromInfo> tableNameList) {
        // 连接查询
        if (tableSource instanceof SQLJoinTableSource) {
            getTableNames(((SQLJoinTableSource) tableSource).getLeft(), tableNameList);
            getTableNames(((SQLJoinTableSource) tableSource).getRight(), tableNameList);
        }
        // 普通表查询
        if (tableSource instanceof SQLExprTableSource) {
            addOnlyTable(tableSource, tableNameList);
        }
    }

    /**
     * 如果当前from语句只有单表，则添加到list中
     *
     * @param tableSource   from语句
     * @param tableNameList 表信息list
     */
    private void addOnlyTable(SQLTableSource tableSource, List<SourceFromInfo> tableNameList) {
        // 普通表查询
        String tableName = String.valueOf(tableSource);
        SourceFromInfo fromInfo = SourceFromInfo.builder()
                .tableName(tableName)
                .alias(tableSource.getAlias())
                .needAddCondition(true)
                .build();
        tableNameList.add(fromInfo);
    }

    /**
     * 处理where中的子查询
     */
    private SQLExpr enhancedWhereSubQuery(SQLExpr where) {
        // 如果where中包含子查询
        if (where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExprWhere = new SQLBinaryOpExpr(MYSQL_STRING);
            binaryOpExprWhere.setLeft(enhancedWhereSubQuery(((SQLBinaryOpExpr) where).getLeft()));
            binaryOpExprWhere.setOperator(((SQLBinaryOpExpr) where).getOperator());
            binaryOpExprWhere.setRight(enhancedWhereSubQuery(((SQLBinaryOpExpr) where).getRight()));
            return binaryOpExprWhere;
        } else {
            // 如果是子查询
            if (where instanceof SQLInSubQueryExpr) {
                doEnhancedWhereSubQuery(where);
            }
            return where;
        }
    }

    private void doEnhancedWhereSubQuery(SQLExpr where) {
        SQLSelect subSelect = ((SQLInSubQueryExpr) where).subQuery;
        // 处理子查询语句
        String newSubQuery = select((MySqlSelectQueryBlock) subSelect.getQueryBlock());
        SQLSelect sqlSelectBySql = getSqlSelectBySql(newSubQuery);
        ((SQLInSubQueryExpr) where).setSubQuery(sqlSelectBySql);
    }


    /**
     * 将String类型select sql语句转化为SQLSelect对象
     *
     * @param sql 查询SQL语句
     * @return 转化后的对象实体
     */
    private SQLSelect getSqlSelectBySql(String sql) {

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, MYSQL_STRING);
        List<SQLStatement> parseStatementList = parser.parseStatementList();
        if (CollectionUtils.isEmpty(parseStatementList)) {
            return null;
        }
        SQLSelectStatement sstmt = (SQLSelectStatement) parseStatementList.get(0);
        return sstmt.getSelect();
    }

}
