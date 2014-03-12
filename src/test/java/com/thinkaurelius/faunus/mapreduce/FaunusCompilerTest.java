package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesMap;
import com.thinkaurelius.faunus.mapreduce.transform.VerticesVerticesMapReduce;
import com.thinkaurelius.faunus.mapreduce.util.CountMapReduce;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompilerTest extends TestCase {

    public void testGlobalConfigurations() {
        FaunusGraph graph = new FaunusGraph();
        graph.getConf().setInt("a_property", 2);
        FaunusCompiler compiler = new FaunusCompiler(graph);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());

        assertEquals(compiler.jobs.size(), 1);
        Job job = compiler.jobs.get(0);

        assertEquals(job.getConfiguration().getInt("a_property", -1), 2);
        assertEquals(job.getConfiguration().getInt("b_property", -1), -1);
        assertEquals(compiler.getConf().getInt("a_property", -1), 2);
        assertEquals(compiler.getConf().getInt("b_property", -1), -1);
    }

    public void testJobListSize() {
        FaunusCompiler compiler = new FaunusCompiler(new FaunusGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 2);
    }

    public void testJobOrder() throws Exception {
        FaunusCompiler compiler = new FaunusCompiler(new FaunusGraph());
        assertEquals(compiler.jobs.size(), 0);
        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);
        compiler.addMapReduce(CountMapReduce.Map.class, null, CountMapReduce.Reduce.class, NullWritable.class, FaunusVertex.class, NullWritable.class, FaunusVertex.class, new Configuration());
        assertEquals(compiler.jobs.size(), 1);

        Job job = compiler.jobs.get(0);

        assertEquals(job.getMapperClass(), ChainMapper.class);
        assertEquals(job.getCombinerClass(), null);
        assertEquals(job.getReducerClass(), ChainReducer.class);
    }

    public void testJobOrder2() throws Exception {
        FaunusPipeline pipe = new FaunusPipeline(new FaunusGraph());
        FaunusCompiler compiler = pipe.getCompiler();
        assertEquals(compiler.jobs.size(), 0);
        pipe.V().out("knows")._();

        assertEquals(compiler.jobs.size(), 1);
        Job job = compiler.jobs.get(0);

        assertEquals(job.getMapperClass(), ChainMapper.class);
        assertEquals(job.getCombinerClass(), null);
        assertEquals(job.getReducerClass(), ChainReducer.class);

        assertEquals(job.getConfiguration().getInt("mapreduce.chain.mapper.size", -1), 2);
        assertEquals(job.getConfiguration().getClass("mapreduce.chain.mapper.mapper.class.0", null), VerticesMap.Map.class);

        assertEquals(job.getConfiguration().getClass("mapreduce.chain.mapper.mapper.class.1", null), VerticesVerticesMapReduce.Map.class);

        Stringifier<JobConf> stringifier = new DefaultStringifier<>(job.getConfiguration(), JobConf.class);
        String[] configStrings = job.getConfiguration().getStrings("mapreduce.chain.mapper.mapper.config.1");

        assertEquals(configStrings.length, 1);
        JobConf jobConf = stringifier.fromString(configStrings[0]);

        assertEquals(jobConf.getStrings(VerticesVerticesMapReduce.LABELS).length, 1);
        assertEquals(jobConf.getStrings(VerticesVerticesMapReduce.LABELS)[0], "knows");

        assertEquals(job.getConfiguration().getInt("mapreduce.chain.reducer.size", -1), 1);
        assertEquals(job.getConfiguration().getClass("mapreduce.chain.reducer.mapper.class.0", null), IdentityMap.Map.class);
        assertEquals(job.getConfiguration().getClass("mapreduce.chain.reducer.reducer.class", null), VerticesVerticesMapReduce.Reduce.class);

    }

    public void testConfigurationPersistence() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 2);
        conf.setBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, false);
        FaunusGraph graph = new FaunusGraph(conf);
        FaunusPipeline pipeline = new FaunusPipeline(graph);
        FaunusCompiler compiler = pipeline.getCompiler();
        TitanOutputFormat outputFormat = new TitanCassandraOutputFormat();

        assertEquals(graph.getConf().getInt("mapreduce.job.reduces", -1), 2);
        assertEquals(compiler.getConf().getInt("mapreduce.job.reduces", -1), 2);
        assertFalse(graph.getConf().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertFalse(compiler.getConf().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        outputFormat.addMapReduceJobs(compiler);

        assertEquals(compiler.jobs.size(), 1);
        Job job = compiler.jobs.get(0);

        assertEquals(job.getConfiguration().getInt("mapreduce.job.reduces", -1), 2);
        assertFalse(job.getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));
        assertEquals(graph.getConf().getInt("mapreduce.job.reduces", -1), 2);
        assertEquals(compiler.getConf().getInt("mapreduce.job.reduces", -1), 2);

        compiler.addMap(IdentityMap.Map.class, NullWritable.class, FaunusVertex.class, IdentityMap.createConfiguration());
        assertEquals(compiler.jobs.size(), 1);

        assertEquals(job.getConfiguration().getInt("mapreduce.job.reduces", -1), 2);
        assertFalse(job.getConfiguration().getBoolean(TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN_INFER_SCHEMA, true));

        assertEquals(graph.getConf().getInt("mapreduce.job.reduces", -1), 2);
        assertEquals(compiler.getConf().getInt("mapreduce.job.reduces", -1), 2);
    }
}
