package day7;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MyLogPartitioner implements Partitioner{

    public MyLogPartitioner(VerifiableProperties props){}
    @Override
    public int partition(Object key, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
}
