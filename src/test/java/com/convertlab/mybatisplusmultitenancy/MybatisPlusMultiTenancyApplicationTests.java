package com.convertlab.mybatisplusmultitenancy;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.convertlab.mybatisplusmultitenancy.mapper.UserMapper;
import com.convertlab.mybatisplusmultitenancy.tenant.ConditionSqlParserInnerInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
@MapperScan("com.convertlab.mybatisplusmultitenancy.mapper")
@Slf4j
class MybatisPlusMultiTenancyApplicationTests {

    @Resource
    private UserMapper userMapper;

    @Test
    public void testSelect1() {
        final LambdaQueryWrapper<User> wrapper = new LambdaQueryWrapper<>();
        userMapper.delete(wrapper.eq(User::getId, 1));
    }

    @Test
    public void testJoin() {
        final long start = System.currentTimeMillis();
        userMapper.getAllMsg("12");
        final long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


    @Test
    public void testSelect() {
        final ConditionSqlParserInnerInterceptor conditionSqlParserInnerInterceptor = new ConditionSqlParserInnerInterceptor();
        conditionSqlParserInnerInterceptor.parserSingle("SELECT t_filter.customer_id FROM (\n" +
                "    SELECT customer_id FROM wechat_fans WHERE   subscribe = 1 ORDER BY customer_id DESC LIMIT 5000\n" +
                ") t_filter INNER JOIN customer_access_control cac\n" +
                "ON  cac.customer_id = t_filter.id\n" +
                "WHERE (cac.access_scope = 0 and (cac.access_code like 'A00001%' or cac.access_code = 'AD')) or\n" +
                "        (cac.access_scope = 1 and (cac.access_code in ('95rzCbT', 'jCf92Pub', 'sSEyezrc') or cac.access_code = 'AG')) or\n" +
                "        (cac.access_scope = 2 and (cac.access_code = '41' or cac.access_code = 'AU')) or\n" +
                "        cac.access_scope = 3\n" +
                "group by customer_id;", null);
    }


    @Test
    public void testInsert() {
        final ConditionSqlParserInnerInterceptor conditionSqlParserInnerInterceptor = new ConditionSqlParserInnerInterceptor();
        conditionSqlParserInnerInterceptor.parserSingle("insert INTO Persons(age,u,b,d) VALUES ('Gates', 'Bill', 'Xuanwumen 10', 'Beijing')", null);
    }


    @Test
    public void TestBatchSave() {
        final ConditionSqlParserInnerInterceptor conditionSqlParserInnerInterceptor = new ConditionSqlParserInnerInterceptor();
        conditionSqlParserInnerInterceptor.parserSingle("\n" +
                "INSERT INTO \n" +
                " \n" +
                "items(name,city,price,number,picture) \n" +
                " \n" +
                "VALUES\n" +
                " \n" +
                "('耐克运动鞋','广州',500,1000,'003.jpg'),\n" +
                " \n" +
                "('耐克运动鞋2','广州2',500,1000,'002.jpg');", null);
    }


    @Test
    public void TestDelete() {
        final ConditionSqlParserInnerInterceptor conditionSqlParserInnerInterceptor = new ConditionSqlParserInnerInterceptor();
        conditionSqlParserInnerInterceptor.parserSingle("delete from user where name in (select a from b)", null);
        userMapper.delete(new LambdaQueryWrapper<User>().eq(User::getId, 1));
        conditionSqlParserInnerInterceptor.parserSingle("delete from user where name in (select a from b)", null);
        userMapper.delete(new LambdaQueryWrapper<User>().eq(User::getId, 1));
        conditionSqlParserInnerInterceptor.parserSingle("delete from user where name in (select a from b)", null);
        userMapper.delete(new LambdaQueryWrapper<User>().eq(User::getId, 1));
        conditionSqlParserInnerInterceptor.parserSingle("delete from user where name in (select a from b)", null);
        userMapper.delete(new LambdaQueryWrapper<User>().eq(User::getId, 1));
    }

    @Test
    public void TestCache() {
        final ConditionSqlParserInnerInterceptor conditionSqlParserInnerInterceptor = new ConditionSqlParserInnerInterceptor(() -> 1L);
        long start, end;
        start = System.currentTimeMillis();
        conditionSqlParserInnerInterceptor.parserSingle("select id from a left join h on h.sex = a.sex  left join (select * from f ,( select * from d ) ) b  on b.name = a.name left join c c1  on c1.name = b.name  where id in  (select * from g where  name in ( select * from k)) union all select * from p", null);
        end = System.currentTimeMillis();
        System.err.println(end - start);
        start = System.currentTimeMillis();
        conditionSqlParserInnerInterceptor.parserSingle("select \n" +
                "    sc.cid as `课程ID`, \n" +
                "    course.cname as `课程名称`,\n" +
                "    count(1) as `选学人数`,\n" +
                "    count(case when score < 60 then null else 1 end) as `合格人数`,\n" +
                "    count(case when score < 60 then 1 else null end) as `不合格人数`,\n" +
                "    round(avg(score)) as `平均成绩`,\n" +
                "    round(count(case when score < 60 then null else 1 end) / count(1), 2) as `通过率`,\n" +
                "    sum(case when score>= 85 then 1 else 0 end) as `100 - 85`,\n" +
                "    sum(case when score>= 70 and score< 85 then 1 else 0 end) as `85 - 70`,\n" +
                "    sum(case when score>= 60 and score<70 then 1 else 0 end) as `70 - 60`,\n" +
                "    sum(case when score< 60 then 1 else 0 end) as `60 - 0`\n" +
                "from sc, course\n" +
                "where sc.Cid = course.Cid\n" +
                "group by sc.Cid", null);
        end = System.currentTimeMillis();
        System.err.println(end - start);

        start = System.currentTimeMillis();
        conditionSqlParserInnerInterceptor.parserSingle("select count(*) from ( select * from b ) ", null);
        end = System.currentTimeMillis();
        System.err.println(end - start);
    }

}
