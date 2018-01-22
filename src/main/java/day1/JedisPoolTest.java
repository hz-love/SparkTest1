package day1;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisPoolTest {
    public static void main(String[] args) {
        //创建jedisPOOL
        JedisPool pool=new JedisPool("Node1",6379);

        //通过pool获取jedis实例
        Jedis jeids = pool.getResource();

        String s1 = jeids.get("s1");
        System.out.println(s1);

        jeids.close();
        pool.close();
    }

}
