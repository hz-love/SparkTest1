package day1;

import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import redis.clients.jedis.Jedis;

import java.util.Scanner;

public class JedisClientText {
    public static void main(String[] args) {
        //创建Jedis
        //host:redis数据库 的ip地址
        // port:redis 数据库的端口号
        Jedis jedis =new Jedis("Node1",6379);
        //通过jedis存储值
        jedis.set("s1","jedis test");
        //通过jedis存储值
        String s2 = jedis.get("s4");
        System.out.println(s2);

        //关闭jedis
        jedis.close();
    }
}
