package com.convertlab.mybatisplusmultitenancy.tenant;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.baomidou.mybatisplus.core.plugins.InterceptorIgnoreHelper;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.baomidou.mybatisplus.core.toolkit.PluginUtils;
import com.baomidou.mybatisplus.extension.plugins.inner.InnerInterceptor;
import com.baomidou.mybatisplus.extension.toolkit.PropertyMapper;
import com.google.common.cache.CacheBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 解析 条件sql
 */
@Slf4j
@Data
@NoArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ConditionSqlParserInnerInterceptor extends DruidParserSupport implements InnerInterceptor {
    private static final String TABLE_FIELD_TENANT_ID = "tenant_id";
    private final static com.google.common.cache.Cache<String, SQLSelectQueryBlock> selectCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(1500, TimeUnit.SECONDS).build();

    private static final DbType MYSQL_STRING = DbType.mysql;

    public ConditionSqlParserInnerInterceptor(TenantLineHandler tenantLineHandler) {
        super(tenantLineHandler);
    }

    @Override
    public void beforeQuery(Executor executor, MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
        if (InterceptorIgnoreHelper.willIgnoreTenantLine(ms.getId())) return;
        PluginUtils.MPBoundSql mpBs = PluginUtils.mpBoundSql(boundSql);
        mpBs.sql(parserSingle(mpBs.sql(), null));
    }


    @Override
    public void beforePrepare(StatementHandler sh, Connection connection, Integer transactionTimeout) {
        PluginUtils.MPStatementHandler mpSh = PluginUtils.mpStatementHandler(sh);
        MappedStatement ms = mpSh.mappedStatement();
        SqlCommandType sct = ms.getSqlCommandType();
        if (sct == SqlCommandType.INSERT || sct == SqlCommandType.UPDATE || sct == SqlCommandType.DELETE) {
            if (InterceptorIgnoreHelper.willIgnoreTenantLine(ms.getId())) return;
            PluginUtils.MPBoundSql mpBs = mpSh.mPBoundSql();
            mpBs.sql(parserMulti(mpBs.sql(), null));
        }
    }

    @Override
    public void setProperties(Properties properties) {
        PropertyMapper.newInstance(properties).whenNotBlank("tenantLineHandler",
                ClassUtils::newInstance, this::setTenantLineHandler);
    }

    @Override
    protected void processInsert(SQLInsertStatement insert, Object obj) {
        final List<SQLInsertStatement.ValuesClause> valuesList = insert.getValuesList();
        insert.addColumn(new SQLIdentifierExpr(TABLE_FIELD_TENANT_ID));
        valuesList.forEach(valuesClause -> valuesClause.addValue(1));
    }

    @Override
    protected void processDelete(SQLDeleteStatement delete, Object obj) {
        final String alias = delete.getAlias();
        final SQLName tableName = delete.getTableName();
        final SourceFromInfo fromInfo = SourceFromInfo.builder().alias(alias).tableName(tableName.getSimpleName()).build();
        final SQLBinaryOpExpr tenantIdCondition = SqlParseUtil.generateTenantIdCondition(fromInfo);
        delete.setWhere(processWhereSubQuery(delete.getWhere()));
        delete.addCondition(tenantIdCondition);
    }

    @Override
    protected void processUpdate(SQLUpdateStatement update, Object obj) {
        final SQLName tableName = update.getTableName();
        final SourceFromInfo fromInfo = SourceFromInfo.builder().tableName(tableName.getSimpleName()).build();
        final SQLBinaryOpExpr tenantIdCondition = SqlParseUtil.generateTenantIdCondition(fromInfo);
        update.setWhere(processWhereSubQuery(update.getWhere()));
        update.addCondition(tenantIdCondition);
    }

    @Override
    protected void processSelect(SQLSelectStatement select, Object obj) {
        SQLSelect sqlSelect = select.getSelect();
        processSelectQuery(sqlSelect.getQuery());
    }


    private void processSelectQuery(SQLSelectQuery query) {
        if (query instanceof SQLUnionQuery) {
            SQLUnionQuery unionQuery = (SQLUnionQuery) query;
            processUnionSelect(unionQuery);
        } else if (query instanceof SQLSelectQueryBlock) {
            processPlainSelect((SQLSelectQueryBlock) query);
        }
    }


    /**
     * 处理基本查询
     *
     * @param
     */
    private void processPlainSelect(SQLSelectQueryBlock select) {
        final String sql = select.toString();
        final SQLSelectQueryBlock sqlSelectQueryBlock = selectCache.getIfPresent(sql);
        if (ObjectUtils.isNotEmpty(sqlSelectQueryBlock)) {
            log.info("使用了缓存---");
            select.setCachedSelectList(sqlSelectQueryBlock.getCachedSelectList(), sqlSelectQueryBlock.getCachedSelectListHash());
            select.setFrom(sqlSelectQueryBlock.getFrom());
            select.setWhere(sqlSelectQueryBlock.getWhere());
            return;
        }
        List<String> tableList = new ArrayList<>();
        // 处理查询字段中的子查询
        processSelectList(select);
        // 处理from语句
        processFrom(select, tableList);
        // 处理where 条件
        processWhere(select, tableList);
        selectCache.put(sql, select);
    }

    private void processSelectList(SQLSelectQueryBlock select) {
        List<SQLSelectItem> selectList = select.getSelectList();
        // 遍历查询的字段，如果查询字段中有子查询
        selectList.forEach(e -> {
            if (e.getExpr() instanceof SQLQueryExpr) {
                SQLQueryExpr expr = (SQLQueryExpr) e.getExpr();
                processSelectQuery(expr.getSubQuery().getQuery());
            }
        });
    }

    public void processFrom(SQLSelectQueryBlock select, List<String> tableList) {
        final SQLTableSource from = select.getFrom();
        // 关联子查询
        if (from instanceof SQLSubqueryTableSource) {
            processSelectQuery(((SQLSubqueryTableSource) from).getSelect().getQuery());
        }
        // 多表 复杂关联
        if (from instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinFrom = (SQLJoinTableSource) from;
            processFromJoin(joinFrom, tableList);
        }
    }

    private void processFromJoin(SQLTableSource joinFrom, List<String> tableList) {
        if (joinFrom != null) {
            if (joinFrom instanceof SQLSubqueryTableSource) {
                processSelectQuery(((SQLSubqueryTableSource) joinFrom).getSelect().getQuery());
            }
            if (joinFrom instanceof SQLJoinTableSource) {
                processFromJoin(((SQLJoinTableSource) joinFrom).getLeft(), tableList);
                processFromJoin(((SQLJoinTableSource) joinFrom).getRight(), tableList);
                // 增强on 语句
                if (((SQLJoinTableSource) joinFrom).getRight() instanceof SQLExprTableSource) {
                    processFromOn((SQLJoinTableSource) joinFrom, tableList);
                }
            }
        }
    }

    /**
     * 处理on的sql
     *
     * @param joinFrom
     * @param tableList
     */
    private void processFromOn(SQLJoinTableSource joinFrom, List<String> tableList) {
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


    public void processWhere(SQLSelectQueryBlock select, List<String> tableList) {
        final SQLExpr where = select.getWhere();
        processWhereSubQuery(where);
        doProcessWhere(select, where, tableList);
    }

    private void doProcessWhere(SQLSelectQueryBlock select, SQLExpr where, List<String> tableList) {
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
        if (org.springframework.util.CollectionUtils.isEmpty(tableNameList)) {
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
     * 处理where中的子查询
     */
    private SQLExpr processWhereSubQuery(SQLExpr where) {
        // 如果where中多个条件
        if (where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExprWhere = new SQLBinaryOpExpr(MYSQL_STRING);
            binaryOpExprWhere.setLeft(processWhereSubQuery(((SQLBinaryOpExpr) where).getLeft()));
            binaryOpExprWhere.setOperator(((SQLBinaryOpExpr) where).getOperator());
            binaryOpExprWhere.setRight(processWhereSubQuery(((SQLBinaryOpExpr) where).getRight()));
            return binaryOpExprWhere;
        } else {
            // 如果是子查询
            if (where instanceof SQLInSubQueryExpr) {
                doProcessWhereSubQuery(where);
            }
            return where;
        }
    }

    private void doProcessWhereSubQuery(SQLExpr where) {
        SQLSelect subSelect = ((SQLInSubQueryExpr) where).subQuery;
        processSelectQuery(subSelect.getQuery());
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
     * 处理Union 查询
     *
     * @param sqlUnionQuery
     */
    private void processUnionSelect(SQLUnionQuery sqlUnionQuery) {
        SQLSelectQuery left = sqlUnionQuery.getLeft();
        SQLSelectQuery right = sqlUnionQuery.getRight();
        if (left instanceof SQLUnionQuery) {
            processUnionSelect((SQLUnionQuery) left);
        } else {
            processPlainSelect((SQLSelectQueryBlock) left);
        }
        if (right instanceof SQLUnionQuery) {
            processUnionSelect((SQLUnionQuery) right);
        } else {
            processPlainSelect((SQLSelectQueryBlock) right);
        }
    }

}

