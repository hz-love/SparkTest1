package day2;

import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

public class BuyCart {
    private static Jedis jedis=new Jedis("Node1",6379);
    private static final String CART="cart";

    public static void testAddItemToCart() {
        jedis.hset(CART+"user03", "肥皂", "2");
        jedis.hset(CART+"user03", "手铐", "1");
        jedis.hset(CART+"user03", "皮鞭", "3");
        jedis.hset(CART+"user03", "蜡烛", "4");


    }
    public static void testGetCartInfo(){
        Map<String,String> cart=jedis.hgetAll(CART+"user03");
        Set<Map.Entry<String, String>> entrySet = cart.entrySet();
        for(Map.Entry<String,String> ent  : entrySet){
            System.out.println(ent.getKey()+":" +ent.getValue());
        }
    }

    public static void editCart(){

        jedis.hincrBy(CART+"user03","蜡烛",1);
    }

    public static void deItemFromCart(){
        jedis.hdel(CART+"user03","手铐");
    }

    public static void main(String[] args) {
        testAddItemToCart();
        testGetCartInfo();
        editCart();
        deItemFromCart();


        jedis.close();

    }
}
