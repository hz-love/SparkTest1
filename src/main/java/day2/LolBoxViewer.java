package day2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

/**
 * Created by liangdmaster on 2017/5/27.
 */
public class LolBoxViewer {
    private static Jedis jedis = null;

    public static void main(String[] args) throws Exception {
        jedis = new Jedis("Node1", 6379);

        int i = 1;

        while (true){
            //每隔3秒查看一次榜单
            Thread.sleep(3000);
            System.out.println("第" + i + "次查看榜单---------------");

            //从redis中查询榜单的前N名
            //Redis Zrevrangebyscore 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
            //具有相同分数值的成员按字典序的逆序(reverse lexicographical order )排列。
            Set<Tuple> topHeros = jedis.zrevrangeWithScores("hero:ccl", 0, 4);

            for (Tuple t : topHeros){
                System.out.println(t.getElement() + "   " + t.getScore());
            }

            i++;

            System.out.println("");
            System.out.println("");
            System.out.println("");
        }
    }
}
