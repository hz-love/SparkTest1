package day2;

import redis.clients.jedis.Jedis;

import java.util.Random;

public class TaskConsumer {
    private static Jedis jedis=new Jedis("Node1",6379);

    public static void main(String[] args) {
        Random r = new Random();
        while (true){
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String taskID = jedis.rpoplpush("task-queue1", "tmp-queue1");
            //处理任务
            if(r.nextInt(19)%9==0){
                //模拟任务失败
                //失败任务,需要从暂存队列弹回任务队列
                jedis.rpoplpush("tmp-queue1", "task-queue1");
                System.out.println("任务失败:"+taskID);

            }else {
                //成功
                jedis.rpop("task-queue1");
                System.out.println("任务成功:"+taskID);
            }
        }
    }
}
