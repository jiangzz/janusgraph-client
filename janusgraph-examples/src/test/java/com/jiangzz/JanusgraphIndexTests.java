package com.jiangzz;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.hadoop.MapReduceIndexManagement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class JanusgraphIndexTests {
    private static final Logger LOGGER= LoggerFactory.getLogger(JanusgraphIndexTests.class);
    private JanusGraph graph;

    @Before
    public void before(){

        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "janusgraph")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "CentOS");

        graph = builder.open();
    }
    @Test
    public void testCompositeIndex() throws InterruptedException, ExecutionException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey  age = mgmt.makePropertyKey("age").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();

        mgmt.buildIndex("name", Vertex.class).addKey(name).buildCompositeIndex();
        mgmt.buildIndex("nameAndAge", Vertex.class).addKey(name).addKey(age).buildCompositeIndex();
        mgmt.commit();
        //Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "name").call();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameAndAge").call();

        //Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("name"), SchemaAction.REINDEX).get();
        mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get();
        mgmt.commit();
    }
    @Test
    public void testCompositeIndexUnique() throws InterruptedException, ExecutionException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey  age = mgmt.makePropertyKey("age").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();

        mgmt.buildIndex("name", Vertex.class).addKey(name).unique().buildCompositeIndex();
        mgmt.buildIndex("nameAndAge", Vertex.class).addKey(name).addKey(age).buildCompositeIndex();
        mgmt.commit();

        //Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "name").call();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameAndAge").call();

        //Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("name"), SchemaAction.REINDEX).get();
        mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get();
        mgmt.commit();
    }


    @Test
    public void testMixedIndex() throws InterruptedException, ExecutionException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").cardinality(Cardinality.SINGLE).dataType(String.class).make();
        PropertyKey  age = mgmt.makePropertyKey("age").cardinality(Cardinality.SINGLE).dataType(Integer.class).make();

        mgmt.buildIndex("nameAndAge1", Edge.class).addKey(name).addKey(age).buildMixedIndex("search");
        mgmt.commit();

        //Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "nameAndAge1").call();
        //Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge1"), SchemaAction.REINDEX).get();
        mgmt.commit();
    }
    @Test
    public void testAddPropertyKey() throws InterruptedException, ExecutionException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey location = mgmt.makePropertyKey("location").cardinality(Cardinality.SINGLE).dataType(Geoshape.class).make();
        JanusGraphIndex nameAndAge = mgmt.getGraphIndex("nameAndAge");
        mgmt.addIndexKey(nameAndAge,location);
        mgmt.commit();
        //Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "nameAndAge").call();
        //Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REINDEX).get();
        mgmt.commit();
    }
    @Test
    public void testOnlyIndexForSpecificVertex() throws InterruptedException, ExecutionException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        VertexLabel person = mgmt.makeVertexLabel("person").make();
        VertexLabel city = mgmt.makeVertexLabel("city").make();

        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();

        //配置属性
        mgmt.addProperties(person,name,age);
        mgmt.addProperties(city,name);

        mgmt.buildIndex("nameIndex",Vertex.class).indexOnly(person).addKey(name).unique().buildCompositeIndex();

        mgmt.commit();
        //Wait for the index to become available
        ManagementSystem.awaitGraphIndexStatus(graph, "nameIndex").call();
        //Reindex the existing data
        mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameIndex"), SchemaAction.REINDEX).get();
        mgmt.commit();
    }

    @Test
    public void updateReIndex() throws BackendException, ExecutionException, InterruptedException {
        JanusGraphManagement mgmt = graph.openManagement();
        MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
        mr.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REMOVE_INDEX).get();
        mgmt.commit();
    }
    @Test
    public void updateDisableIndex() throws BackendException, ExecutionException, InterruptedException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex("nameAndAge"),SchemaAction.DISABLE_INDEX).get();
        mgmt.commit();
        graph.tx().commit();
        ManagementSystem.awaitGraphIndexStatus(graph, "nameAndAge").status(SchemaStatus.DISABLED).call();

    }
    @Test
    public void updateRemoveIndex() throws BackendException, ExecutionException, InterruptedException {
        graph.tx().rollback(); //Never create new indexes while a transaction is active
        JanusGraphManagement mgmt = graph.openManagement();

        MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
        JanusGraphManagement.IndexJobFuture future = mr.updateIndex(mgmt.getGraphIndex("nameAndAge"), SchemaAction.REMOVE_INDEX);
        mgmt.commit();
        graph.tx().commit();
        future.get();
    }
    @After
    public void after(){
        graph.close();
    }
}
