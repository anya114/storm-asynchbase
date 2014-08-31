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
 * Fill an HBase table with a fixed 1KB payload.
 * RandomKeyValueSpout emit random rowKey/qualifier
 * as fast as possible.
 * <p/>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class AsyncHBaseBoltExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseBoltExampleTopology.class);

    public static StormTopology buildTopology() {

        class PrinterBolt extends BaseBasicBolt {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

            @Override
            public void execute(Tuple tuple, BasicOutputCollector collector) {
                log.info(tuple.toString());
            }
        }

        class IntegerSerializer implements AsyncHBaseSerializer {
            @Override
            public byte[] serialize(Object object) {
                return Integer.toString((int) object).getBytes();
            }

            @Override
            public void prepare(Map conf) {

            }
        }

        RandomKeyValueSpout randomKeyValueSpout = new RandomKeyValueSpout();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", randomKeyValueSpout, 1);
        String payload = new String(new char[1024]).replace('\0', 'x');

        IAsyncHBaseMapper mapper = new AsyncHBaseMapper()
            .addFieldMapper(new AsyncHBaseFieldMapper()
                    .setTable("test")
                    .setRowKeyField("key")
                    .setColumnFamily("data")
                    .setColumnQualifierField("value")
                    .setColumnQualifierSerializer(new IntegerSerializer())
                    .setValue(payload)
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
