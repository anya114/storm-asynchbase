/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

// TODO : handle tuple replay
public class RandomKeyValueSpout extends BaseRichSpout {
    public static Logger log = LoggerFactory.getLogger(RandomKeyValueSpout.class);
    SpoutOutputCollector collector;

    List<Object> keys;
    List<Object> values;
    int sleep = -1;
    Random generator;

    public RandomKeyValueSpout() {
    }

    public Object nextKey() {
        if (keys != null) {
            return keys.get(generator.nextInt(keys.size()));
        } else {
            return "key-" + generator.nextLong();
        }
    }

    public Object nextValue() {
        if (values != null) {
            return values.get(generator.nextInt(keys.size()));
        } else {
            return generator.nextInt();
        }
    }

    public RandomKeyValueSpout setKeys(List<Object> keys) {
        this.keys = keys;
        return this;
    }


    public RandomKeyValueSpout setValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public RandomKeyValueSpout setSleep(int sleep) {
        this.sleep = sleep;
        return this;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.generator = new Random();
    }

    @Override
    public void nextTuple() {
        if (this.sleep > 0) {
            try {
                Time.sleep(this.sleep);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        this.collector.emit(new Values(this.nextKey(), this.nextValue()), new Object());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
