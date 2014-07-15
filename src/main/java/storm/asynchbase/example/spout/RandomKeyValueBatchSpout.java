/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.*;

// TODO : handle batch replay
public class RandomKeyValueBatchSpout implements IBatchSpout {
    final int batchSize;
    HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
    protected List<Object> keys;
    protected List<Object> values;
    protected Random generator;
    int index = 0;
    int sleep = -1;

    public RandomKeyValueBatchSpout(int batchSize) {
        this.batchSize = batchSize;
    }

    public RandomKeyValueBatchSpout() {
        this(10);
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

    public RandomKeyValueBatchSpout setKeys(List<Object> keys) {
        this.keys = keys;
        return this;
    }


    public RandomKeyValueBatchSpout setValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public RandomKeyValueBatchSpout setSleep(int sleep) {
        this.sleep = sleep;
        return this;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
        this.generator = new Random();
    }

    @Override
    public void emitBatch(long batchId, TridentCollector
        collector) {
        if (this.sleep > 0) {
            try {
                Time.sleep(this.sleep);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        List<List<Object>> batch = this.batches.get(batchId);
        if (batch == null) {
            batch = new ArrayList<List<Object>>();
            for (int i = 0; i < this.batchSize; i++) {
                batch.add(new Values(this.nextKey(), this.nextValue()));
            }
            this.batches.put(batchId, batch);
        }
        for (List<Object> list : batch) {
            collector.emit(list);
        }
    }

    @Override
    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("key", "value");
    }
}
