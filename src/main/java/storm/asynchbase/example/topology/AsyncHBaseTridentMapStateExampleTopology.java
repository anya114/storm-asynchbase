/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.example.spout.RandomKeyValueBatchSpout;
import storm.asynchbase.trident.state.AsyncHBaseMapState;
import storm.asynchbase.utils.serializer.AsyncHBaseSerializer;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.TransactionalValue;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This topology use Trident sums up the random values emited by the spout for each key.<br/>
 * And then persisit it to HBase using AsyncHBaseMapState.<br/>
 * A DRPC query the state every second.<br/>
 * </p>
 * <p>
 * You'll have to set hBaseConfig and to tweak parallel hints and config
 * to match your configuration.
 * </p>
 */
public class AsyncHBaseTridentMapStateExampleTopology {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseTridentMapStateExampleTopology.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static StormTopology buildTopology(LocalDRPC drpc) {

        class AsyncHBaseLongSerializer implements AsyncHBaseSerializer {
            @Override
            public byte[] serialize(Object value) {
                return Long.toString((long) value).getBytes();
            }

            @Override
            public void prepare(Map conf) {

            }
        }

        class MyRandomKeyValueBatchSpout extends RandomKeyValueBatchSpout {
            @Override
            public Object nextValue() {
                return generator.nextInt(10);
            }
        }

        /**
         * Topology
         */

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("stream", new MyRandomKeyValueBatchSpout().setKeys(new Values("foo", "bar", "baz")));

        /**
         * Partition sum
         */

        AsyncHBaseMapState.Options sumStateOptions = new AsyncHBaseMapState.Options<TransactionalValue>();
        sumStateOptions.cluster = "hbase-cluster";
        sumStateOptions.table = "test";
        sumStateOptions.columnFamily = "data";
        sumStateOptions.columnQualifier = "total";

        TridentState sumState = stream
            .groupBy(new Fields("key"))
            .persistentAggregate(AsyncHBaseMapState.transactional(sumStateOptions), new Fields("value"), new Sum(), new Fields("sum")).parallelismHint(10);

        sumState
            .newValuesStream()
            .each(new Fields("key","sum"), new Debug());

        topology.newDRPCStream("sum-drpc", drpc)
            .stateQuery(sumState, new Fields("args"), new MapGet(), new Fields("sum"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        Map<String, String> hBaseConfig = new HashMap<>();
        hBaseConfig.put("zkQuorum", "node-00113.hadoop.ovh.net,node-00114.hadoop.ovh.net,node-00116.hadoop.ovh.net");
        conf.put("hbase-cluster", hBaseConfig);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(5);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        } else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HBaseTridentExampleTopology", conf, buildTopology(drpc));
            while (true) {
                log.info("drpc sum : " + drpc.execute("sum-drpc", "foo"));
                Thread.sleep(1000);
            }
        }
    }
}