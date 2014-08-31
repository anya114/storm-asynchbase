/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.bolt.AsyncHBaseBolt;
import storm.asynchbase.bolt.mapper.AsyncHBaseFieldMapper;
import storm.asynchbase.bolt.mapper.AsyncHBaseMapper;
import storm.asynchbase.bolt.mapper.IAsyncHBaseMapper;
import storm.asynchbase.example.spout.RandomKeyValueSpout;
import storm.asynchbase.utils.serializer.AsyncHBaseSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This topology shows how to use dynamic qualifiers/values mapping
 * <p/>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class AsyncHBaseBoltMapExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseBoltMapExampleTopology.class);

    public static StormTopology buildTopology() {

        class PrinterBolt extends BaseBasicBolt {
            private int mapSize = 10;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        }

        class IntegerSerializer implements AsyncHBaseSerializer {

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                log.info(tuple.toString());
            }

            @Override
            public byte[] serialize(Object object) {
                return Integer.toString((int) object).getBytes();
            }
        }

        class MyRandomKeyValueSpout extends RandomKeyValueSpout {

            @Override
            public void prepare(Map conf) {

            }

            public MyRandomKeyValueSpout setMapSize(int mapSize) {
                this.mapSize = mapSize;
                return this;
            }

            @Override
            public Object nextValue() {
                Map<Object, Object> map = new HashMap<>(mapSize);
                for (int i = 0; i < mapSize; i++) {
                    map.put(super.nextKey(), super.nextValue());
                }
                return map;
            }
        }

        RandomKeyValueSpout myRandomKeyValueSpout = new MyRandomKeyValueSpout().setSleep(200);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", myRandomKeyValueSpout, 1);

        IAsyncHBaseMapper mapper = new AsyncHBaseMapper()
            .addFieldMapper(new AsyncHBaseFieldMapper()
                    .setTable("test2")
                    .setRowKeyField("key")
                    .setColumnFamily("data")
                    .setMapField("value")
                    .setValueSerializer(new IntegerSerializer())
            );

        builder.setBolt(
            "hbase-bolt",
            new AsyncHBaseBolt("hbase-cluster", mapper),
            5).noneGrouping("spout");

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        Map<String, String> hBaseConfig = new HashMap<>();
        hBaseConfig.put("zkQuorum", "node-00113.hadoop.ovh.net,node-00114.hadoop.ovh.net,node-00116.hadoop.ovh.net");
        conf.put("hbase-cluster", hBaseConfig);

        conf.setMaxSpoutPending(5000);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(5);
            StormSubmitter.submitTopology(args[0], conf, buildTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HBaseValueStateExampleTopology", conf, buildTopology());
        }
    }
}
