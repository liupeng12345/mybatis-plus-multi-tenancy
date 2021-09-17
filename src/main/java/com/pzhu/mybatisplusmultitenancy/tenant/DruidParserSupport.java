package com.pzhu.mybatisplusmultitenancy.tenant;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.h2.parser.H2StatementParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.baomidou.mybatisplus.core.toolkit.StringPool;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class DruidParserSupport {
    private TenantLineHandler tenantLineHandler;

    public String parserSingle(String sql, Object obj) {
        deBug(sql, "original");
        Long start,end;
        start = System.currentTimeMillis();
        H2StatementParser parser = new H2StatementParser(sql);
        end = System.currentTimeMillis();
        System.err.println(end-start);
        SQLStatement stmt = parser.parseStatement();
        return processParser(stmt, obj);
    }

    protected String processParser(SQLStatement stmt, Object obj) {
        // 查询语句
        if (stmt instanceof SQLSelectStatement) {
            processSelect((SQLSelectStatement) stmt, obj);
        }
        // 更新语句
        else if (stmt instanceof SQLUpdateStatement) {
            processUpdate((SQLUpdateStatement) stmt, obj);
        }
        //删除语句
        else if (stmt instanceof SQLDeleteStatement) {
            processDelete((SQLDeleteStatement) stmt, obj);
        }
        //插入语句
        else if (stmt instanceof SQLInsertStatement) {
            processInsert((SQLInsertStatement) stmt, obj);
        }
        String sql = stmt.toString();
        deBug(sql, "after process");
        final String replaceSql = sql.replace(String.valueOf(Long.MIN_VALUE), String.valueOf(tenantLineHandler.getTenantId()));
        deBug(replaceSql, "replaceSql");
        return replaceSql;
    }

    public String parserMulti(String sql, Object obj) {
        deBug(sql, "original");
        final MySqlStatementParser parser = new MySqlStatementParser(sql);
        final List<SQLStatement> statements = parser.parseStatementList();
        return statements.stream().map(sqlStatement -> processParser(sqlStatement, obj)).collect(Collectors.joining(StringPool.SEMICOLON));
    }

    private void deBug(String sql, String prefix) {
        if (log.isDebugEnabled()) {
            log.debug("{} sql:{}", prefix, sql);
        }
    }


    /**
     * 新增
     */
    protected void processInsert(SQLInsertStatement insert, Object obj) {
        throw new UnsupportedOperationException();
    }

    /**
     * 删除
     */
    protected void processDelete(SQLDeleteStatement delete, Object obj) {
        throw new UnsupportedOperationException();
    }

    /**
     * 更新
     */
    protected void processUpdate(SQLUpdateStatement update, Object obj) {
        throw new UnsupportedOperationException();
    }

    /**
     * 查询
     */
    protected void processSelect(SQLSelectStatement select, Object obj) {
        throw new UnsupportedOperationException();
    }

}
