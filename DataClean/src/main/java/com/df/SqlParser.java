package com.df;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import jdk.nashorn.internal.runtime.ParserException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class SqlParser {
    public static Map<String, TreeSet<String>> getFromTo (String sql) throws ParserException {
        Map<String, TreeSet<String>> result = new HashMap<String, TreeSet<String>>();
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        TreeSet<String> selectSet = new TreeSet<String>();
        TreeSet<String> updateSet = new TreeSet<String>();
        TreeSet<String> insertSet = new TreeSet<String>();
        TreeSet<String> deleteSet = new TreeSet<String>();

        if (stmts == null) {
            return null;
        }

        String database = "DEFAULT";
        for (SQLStatement stmt : stmts) {
            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(stmts,JdbcConstants.HIVE);
            if (stmt instanceof SQLUseStatement) {
                database = ((SQLUseStatement) stmt).getDatabase().getSimpleName();
            }
            stmt.accept(statVisitor);
            Map<TableStat.Name, TableStat> tables = statVisitor.getTables();

            if (tables != null) {
                final String db = database;
                for (Map.Entry<TableStat.Name, TableStat> table : tables.entrySet()) {
                    TableStat.Name tableName = table.getKey();
                    TableStat stat = table.getValue();

                    if (stat.getCreateCount() > 0 || stat.getInsertCount() > 0) { //create
                        String insert = tableName.getName();
                        if (!insert.contains("."))
                            insert = db + "." + insert;
                        insertSet.add(insert);
                    } else if (stat.getSelectCount() > 0) { //select
                        String select = tableName.getName();
                        if (!select.contains("."))
                            select = db + "." + select;
                        selectSet.add(select);
                    }else if (stat.getUpdateCount() > 0 ) { //update
                        String update = tableName.getName();
                        if (!update.contains("."))
                            update = db + "." + update;
                        updateSet.add(update);
                    }else if (stat.getDeleteCount() > 0) { //delete
                        String delete = tableName.getName();
                        if (!delete.contains("."))
                            delete = db + "." + delete;
                        deleteSet.add(delete);
                    }
                }
            }
        }

        result.put("select",selectSet);
        result.put("insert",insertSet);
        result.put("update",updateSet);
        result.put("delete",deleteSet);

        return result;
    }

    public static void main(String[] args) {
        String sql = " select   date_sub(current_date,1) as datemion,\n" +
                "           nvl(org.dept_code,'04xxxxxxx') , \n" +
                "           nvl(sum(ts.zaiku_amount_1),0) * 100 as zaiku_amount_1,\n" +
                "           nvl(sum(ts.zaiku_amount_2),0) as zaiku_amount_2,\n" +
                "           nvl(sum(ts.baddebt_amount),0) as BADDEBT_AMOUNT ,\n" +
                "           \n" +
                "           date_format(current_timestamp,'yyyy-MM-dd HH:mm:ss') as op_time\n" +
                "           , date_sub(current_date,1)  as strdate\n" +
                "    from\n" +
                "     (select  '04xxxxxxx' as strdeptcode\n" +
                "          ,nvl((sum(borrow_amount) - sum(repay_principal)),0) as zaiku_amount_1,0 as zaiku_amount_2  , 0 as baddebt_amount\n" +
                "        from\n" +
                "        (select nvl(sum(amount),0) as borrow_amount,0 as repay_principal\n" +
                "            from dw.dw_telcustomerapply\n" +
                "          where applystatus = '2'\n" +
                "            and amount >= 0\n" +
                "            and to_date(successtime) < current_date\n" +
                "            and to_date(successtime) is not null\n" +
                "       union all\n" +
                "       select 0 as borrow_amount,nvl(sum(principalmoney),0) as repay_principal          --还款本金\n" +
                "          from dw.dw_telrepayment\n" +
                "        where realrepaytime is not null\n" +
                "          and to_date(realrepaytime) < current_date\n" +
                "          and repaystatus = '2'\n" +
                "      ) ts1\n" +
                "    union all\n" +
                "    select  strdeptcode\n" +
                "           , 0 as zaiku_amount_1\n" +
                "           , nvl(sum(nvl(loan_amount_notstage,0) + nvl(loan_amount_stage,0) - nvl(repay_capital_amount_stage,0)\n" +
                "           - nvl(repay_capital_amount_notstage,0) - nvl(fenqi_amount,0)),0) as zaiku_amount_2\n" +
                "           , 0 as baddebt_amount\n" +
                "      from (\n" +
                "           select strdeptcode,0 as loan_amount_notstage,0 as loan_amount_stage,\n" +
                "                  nvl(sum(case when b.nborrowmode not in (23,111) and nState=9 then lPrincipal else 0 end),0) as repay_capital_amount_notstage, --还款本金(不包含转分期),在库净增需要用到\n" +
                "                  nvl(sum(case when b.nborrowmode in (23,111) and nState=9 then lPrincipal else 0 end),0) as repay_capital_amount_stage, --还款本金(转分期的),在库净增需要用到\n" +
                "                  sum(case when b.nState in (32) then lPrincipal else 0 end) as fenqi_amount\n" +
                "             from\n" +
                "                  (select ncode from dw.dw_tbfundprovider where nbusinesstype = 0) fund\n" +
                "                   inner join\n" +
                "                  (select lId,nBorrowMode from dw.dw_tbborrowintent\n" +
                "                        where nState in (4,5,7,9,32)\n" +
                "                  ) a\n" +
                "                  on fund.ncode = a.nBorrowMode\n" +
                "                inner join\n" +
                "                  (select lBorrowIntentId,strdeptcode,lPrincipal,nState,nborrowmode  \n" +
                "                     from dw.dw_tbBorrowerBill\n" +
                "                    where nState in (9,32) and lPrincipal > 0\n" +
                "                    and strRealRepayDate < current_date --还款日期条件\n" +
                "                  ) b\n" +
                "                  on a.lId=b.lBorrowIntentId\n" +
                "                  group by b.strdeptcode\n" +
                "           union all\n" +
                "           select strdeptcode\n" +
                "                 , nvl(sum(case when nborrowmode not in (23,111) then lAmount end),0) as loan_amount_notstage --不含转分期的，需要计算在库净增\n" +
                "                 , nvl(sum(case when nborrowmode in (23,111) then lAmount end),0) as loan_amount_stage --转分期的，需要计算在库净增\n" +
                "                 ,0 as repay_capital_amount_notstage\n" +
                "                 ,0 as repay_capital_amount_stage\n" +
                "                 ,0 as fenqi_amount\n" +
                "             from (select ncode from dw.dw_tbfundprovider where nbusinesstype = 0) fund\n" +
                "                  inner join\n" +
                "                  (select nBorrowMode,lAmount,strdeptcode from dw.dw_tbborrowintent\n" +
                "                      where strLoanDate is not null\n" +
                "                        and substr(strLoanDate,0,10) < current_date\n" +
                "                        and nState in (4,5,7,9,32)\n" +
                "                        and lAmount > 0\n" +
                "                   ) a\n" +
                "                on fund.ncode = a.nBorrowMode\n" +
                "            group by a.strdeptcode\n" +
                "        ) ts2\n" +
                "        group by ts2.strdeptcode\n" +
                "        \n" +
                "   union all\n" +
                "     select deptcode as  strdeptcode\n" +
                "           , 0 as zaiku_amount_1\n" +
                "           , 0 as zaiku_amount_2\n" +
                "           , nvl(sum(case when baddebt.strdate = date_sub(current_date,1) then baddebt.bad_debt end ),0)\n" +
                "             - nvl(sum(case when baddebt.strdate = date_sub(current_date,2) then baddebt.bad_debt end ),0) as baddebt_amount\n" +
                "     from  app.app_dept_dailyprofit_baddebt_accu baddebt\n" +
                "         group by baddebt.deptcode\n" +
                "    ) ts\n" +
                "   left join dim.organization_dim_custdepart org on org.dept_code= ts.strdeptcode \n" +
                "   group by nvl(org.dept_code,'04xxxxxxx')\n" +
                "  distribute by strdate ";

        //sql = "update supindb.college set uid='22333' where name='小明'";
        //sql = "delete from supindb.college where uid= '22223333'";

        Map<String, TreeSet<String>> getfrom = getFromTo(sql);

        for (Map.Entry<String, TreeSet<String>> entry : getfrom.entrySet()){
            System.out.println("================");
            System.out.println("key=" + entry.getKey());
            for (String table : entry.getValue()){
                System.out.println(table);
            }
        }
    }
}
