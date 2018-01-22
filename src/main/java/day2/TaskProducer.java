package day2;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.UUID;

public class TaskProducer {
    private static Jedis jedis=new Jedis("Node1",6379);
    public static void main(String[] args) {
        Random r=new Random();
        while (true){
            int nex = r.nextInt(1000);
            try {
                Thread.sleep(1000+nex);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String taskID = UUID.randomUUID().toString();
            jedis.lpush("task-queue1",taskID);
            System.out.println("生成了一个任务:"+taskID);
        }
    }
}
