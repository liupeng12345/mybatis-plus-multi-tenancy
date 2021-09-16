package com.convertlab.mybatisplusmultitenancy.tenant;

import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public  class ExportTableAliasVisitor extends MySqlASTVisitorAdapter {
    @Override
    public boolean visit(SQLJoinTableSource.UDJ x) {
        return super.visit(x);
    }
}