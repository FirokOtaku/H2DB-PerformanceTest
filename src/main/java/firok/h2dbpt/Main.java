package firok.h2dbpt;

import firok.topaz.general.ProfileTimer;
import org.h2.jdbcx.JdbcConnectionPool;

import java.io.File;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        System.out.println("开始执行");
        var fileDatabase = new File("./h2db-pt").getCanonicalFile().getAbsoluteFile();
        var jcp = JdbcConnectionPool.create("jdbc:h2:" + fileDatabase, "sa", "sa");
        try
        {
            try(var conn = jcp.getConnection();
                )
            {
                try(var stmt = conn.prepareStatement("""
create table if not exists d_user (
id varchar(48) not null unique,
username varchar(64) not null,
password varchar(64) not null,
gender bool,
birthday date,
primary key (id)
)
"""))
                {
                    var rsCreateUserTable = stmt.executeUpdate();
                    System.out.println("创建用户信息表: " + rsCreateUserTable);
                }

                try(var stmtQuery = conn.prepareStatement("select count(1) as COUNT from d_user");
                    var rsCountUserTable = stmtQuery.executeQuery())
                {
                    System.out.println("数量查询: " + rsCountUserTable.next() + "," + rsCountUserTable.getFetchSize());
                    var count = rsCountUserTable.getInt("COUNT");
                    System.out.println("数量: " + count);
                }

                var rand = new Random();

                // 开 4 个线程试试效果
                IntStream.range(0, 4).parallel().forEach(indexThread -> {
                    var threadName = "子线程 - " + indexThread + " ";
                    Thread.currentThread().setName(threadName.trim());
                    var timer = new ProfileTimer(threadName + "启动");

                    try(var stmtInsert = conn.prepareStatement("""
insert into d_user
(id, username, password, gender, birthday)
values (?, ?, ?, ?, ?)
"""))
                    {
                        var countInsert = 0;
                        for(var step = 0; step < 100000; step++)
                        {
                            var id = UUID.randomUUID().toString();
                            var username = "user-" + id.substring(0, 8);
                            var password = "user-" + id.substring(0, 16);
                            var gender = rand.nextBoolean();
                            var birthday = new java.sql.Date(rand.nextLong());
                            stmtInsert.setString(1, id);
                            stmtInsert.setString(2, username);
                            stmtInsert.setString(3, password);
                            stmtInsert.setBoolean(4, gender);
                            stmtInsert.setDate(5, birthday);

                            countInsert += stmtInsert.executeUpdate();
                        }

                        timer.check(threadName + "完成创建 ~ " + countInsert);
                    }
                    catch (Exception any)
                    {
                        any.printStackTrace(System.err);
                        timer.check(threadName + "出错");
                    }

                    timer.check(threadName + "结束");

                    System.out.println(threadName + " 执行情况 --\n" + timer.snapshot());
                });

                try(var stmtCompact = conn.prepareStatement("shutdown compact"))
                {
                    stmtCompact.execute();
                    System.out.println("压缩完成");
                }
            }
        }
        catch (Exception any)
        {
            System.out.println("执行出错");
            any.printStackTrace(System.err);
        }
        jcp.dispose();
    }
}
