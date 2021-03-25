package com.jiangzz;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.Map;

public class JanusgraphSchemaTests {
    private static final Logger LOGGER= LoggerFactory.getLogger(JanusgraphSchemaTests.class);
    private JanusGraph janusGraph;

    @Before
    public void before(){
        LOGGER.debug("=========== 开始创建JanusGraph =============");

        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "janusgraph")
                .set("index.search.backend", "elasticsearch")
                //.set("schema.default", "none")
                //.set("schema.constraints", true)
                .set("index.search.hostname", "CentOS");

        janusGraph = builder.open();
    }

    @Test
    public void testShowSchema(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //边Label信息
        String edgeLabels = mgmt.printEdgeLabels();
        //顶点Label信息
        String vertexLabels = mgmt.printVertexLabels();
        //属性信息
        String propertyKeys = mgmt.printPropertyKeys();
        //索引信息
        String indexes = mgmt.printIndexes();
        String schema = mgmt.printSchema();

        LOGGER.debug("边标签：\n{}",edgeLabels);
        LOGGER.debug("顶点标签：\n{}",vertexLabels);
        LOGGER.debug("属性propertyKeys：\n{}",propertyKeys);
        LOGGER.debug("索引信息：\n{}",indexes);
        //LOGGER.debug("所有的schema：\n{}",schema);
    }
    @Test
    public void testCreateEdgeLabels(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //默认开启事物
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("husband").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.commit();
    }
    @Test
    public void testCreatePropertyKeys(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //默认开启事物
        mgmt.makePropertyKey("birthDate").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.makePropertyKey("sensorReading").dataType(Double.class).cardinality(Cardinality.LIST).make();
        mgmt.commit();
    }
    @Test
    public void testChangeSchema() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        PropertyKey birthDate = mgmt.getPropertyKey("birthDate");
        mgmt.changeName(birthDate,"birthDay");
        mgmt.commit();
    }

    @Test
    public void testRelationTypes(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        if (mgmt.containsRelationType("name")) {
            PropertyKey name = mgmt.getPropertyKey("name");
            LOGGER.info("label {} name {}",name.label(),name.name());
        }
        Iterable<EdgeLabel> relationTypes = mgmt.getRelationTypes(EdgeLabel.class);
        for (EdgeLabel relationType : relationTypes) {
            LOGGER.info("label {} name {}",relationType.label(),relationType.name());
        }
        mgmt.commit();
    }
    @Test
    public void testCreateVertexLabels() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //创建一个person Label
        VertexLabel personLabel = mgmt.makeVertexLabel("person").make();
        mgmt.commit();
    }
    @Test
    public void testBindMultiProperties2Vertex() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //获取现有属性定义
        PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
        PropertyKey name = mgmt.getPropertyKey("name");
        //获取顶点标签
        VertexLabel personLabel = mgmt.getVertexLabel("person");
        //绑定标签属性
        mgmt.addProperties(personLabel,birthDay,name);

        mgmt.commit();
    }
    @Test
    public void testBindMultiProperties2Edge() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //获取现有属性定义
        PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
        //获取顶点标签
        EdgeLabel mother = mgmt.getEdgeLabel("mother");
        //绑定标签属性
        mgmt.addProperties(mother,birthDay);
        mgmt.commit();
    }@Test
    public void testAddConnectionConstraints() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        VertexLabel personLabel = mgmt.getVertexLabel("person");
        EdgeLabel motherEdgeLabel = mgmt.getEdgeLabel("mother");

        mgmt.addConnection(motherEdgeLabel,personLabel,personLabel);
        mgmt.commit();
    }

    @Test
    public void testCreateVertex(){
        VertexLabel personLabel = janusGraph.getVertexLabel("person");
        JanusGraphVertex personVertex = janusGraph.addVertex(T.label,personLabel,"name","李四妈妈","birthDay",System.currentTimeMillis());
        janusGraph.tx().commit();
    }
    @Test
    public void testCreateEdge(){
        EdgeLabel motherEdgeLabel = janusGraph.getEdgeLabel("mother");

        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex lisi = traversal.V().has("name", "李四").next();
        Vertex lisiMother = traversal.V().has("name", "李四妈妈").next();
        lisi.addEdge("mother",lisiMother,"birthDay",System.currentTimeMillis());
        traversal.tx().commit();
    }

    @Test
    public void testStaticTable(){
        JanusGraphManagement mgmt = janusGraph.openManagement();

        VertexLabel tweet = mgmt.makeVertexLabel("tweet").setStatic().make();

        PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
        PropertyKey sensorReading = mgmt.getPropertyKey("sensorReading");

        mgmt.addProperties(tweet,birthDay,sensorReading);
        mgmt.commit();
    }
    @Test
    public void testUpdateStaticTable01(){
         janusGraph.addVertex(T.label, "tweet", "birthDay", System.currentTimeMillis());
         janusGraph.tx().commit();
    }
    @Test
    public void testUpdateStaticTable02(){
        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex tweet = traversal.V().has(T.label, "tweet").next();
        //System.out.println(tweet.<Long>value("birthDay"));
        //1616665131192
        tweet.property("birthDay",System.currentTimeMillis());
        janusGraph.tx().commit();
    }
    @Test
    public void testEdgeTTL01(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        VertexLabel person = mgmt.getVertexLabel("person");
        EdgeLabel visits = mgmt.makeEdgeLabel("visits").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.setTTL(visits, Duration.ofSeconds(10));

        mgmt.addConnection(visits,person,person);
        mgmt.commit();
    }
    @Test
    public void testTTL02(){

        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex lisi = traversal.V().has("name", "李四").next();
        Vertex lisiMother = traversal.V().has("name", "李四妈妈").next();
        lisi.addEdge("visits",lisiMother);

        traversal.tx().commit();
    }
    @Test
    public void testTTL03(){

        GraphTraversalSource traversal = janusGraph.traversal();
        GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = traversal.V().outE("visits").inV().valueMap("name");

        System.out.println(vertexMapGraphTraversal);
    }

    @Test
    public void testDropSchema() throws BackendException {
        JanusGraphFactory.drop(janusGraph);
    }
    @After
    public void after(){
        LOGGER.debug("===========关闭JanusGraph实例 =============");
        janusGraph.close();
    }
}
