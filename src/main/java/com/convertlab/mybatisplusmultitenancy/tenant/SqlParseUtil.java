package com.convertlab.mybatisplusmultitenancy.tenant;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

public class SqlParseUtil {

    private static final String TABLE_FIELD_TENANT_ID = "tenant_id";

    private static final DbType MYSQL_STRING = DbType.mysql;

    /**
     * 生成 sql 中的 条件
     *
     * @param left              条件左边
     * @param right             条件右边
     * @param sqlBinaryOperator 操作符
     * @return 返回条件语句
     */
    public static SQLExpr generateCondition(SQLExpr left, SQLExpr right, SQLBinaryOperator sqlBinaryOperator) {
        // 左边为空直接返回右边
        if (ObjectUtils.isEmpty(left)) {
            return right;
        }
        if (ObjectUtils.isEmpty(right)) {
            return left;
        }
        final SQLBinaryOpExpr condition = new SQLBinaryOpExpr(MYSQL_STRING);
        condition.setLeft(left);
        condition.setOperator(sqlBinaryOperator);
        condition.setRight(right);
        return condition;
    }

    public static SQLBinaryOpExpr generateTenantIdCondition(SourceFromInfo tableNameInfo) {
        SQLBinaryOpExpr tenantIdWhere = new SQLBinaryOpExpr(MYSQL_STRING);
        if (StringUtils.isEmpty(tableNameInfo.getAlias())) {
            // 拼接新的条件
            tenantIdWhere.setLeft(new SQLPropertyExpr(tableNameInfo.getTableName(), TABLE_FIELD_TENANT_ID));
            // 设置当前租户ID条件
        } else {
            // 拼接别名条件
            tenantIdWhere.setLeft(new SQLPropertyExpr(tableNameInfo.getAlias(), TABLE_FIELD_TENANT_ID));
        }
        tenantIdWhere.setOperator(SQLBinaryOperator.Equality);
        tenantIdWhere.setRight(new SQLIntegerExpr(Long.MIN_VALUE));
        return tenantIdWhere;
    }

    /**
     * 获取关联表的名字
     *
     * @return
     */
    public static String getTableName(SQLExprTableSource sqlTableSource) {
        final String alias = sqlTableSource.getAlias();
        if (StringUtils.isNotBlank(alias)) {
            return alias;
        }
        return sqlTableSource.getName().getSimpleName();
    }

}
