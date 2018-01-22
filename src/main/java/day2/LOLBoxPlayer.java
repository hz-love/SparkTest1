package day2;
import redis.clients.jedis.Jedis;

import java.util.Random;

/**
 * Created by liangdmaster on 2017/5/27.
 */
public class LOLBoxPlayer {
    private static Jedis jedis = null;

    public static void main(String[] args) throws Exception {
        jedis = new Jedis("Node1", 6379);

        Random r = new Random();

        String[] heros = {"易大师","德邦","剑姬","盖伦","阿卡丽","金克斯","提莫","蒙多","猴子","亚索"};

        while (true){
            int index = r.nextInt(heros.length);
            //选择一个英雄
            String hero = heros[index];
            //开始玩游戏
            Thread.sleep(1000);

            //给集合中的英雄的每出场一次加一
            //第一次添加的时候，集合不存在，zincrby方法会创建
            jedis.zincrby("hero:ccl", 1, hero);

            System.out.println(hero + " 出场了.....");

        }
    }

}

