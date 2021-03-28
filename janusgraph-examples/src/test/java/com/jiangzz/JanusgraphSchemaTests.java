package com.jiangzz;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

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
       // mgmt.makePropertyKey("birthDate").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
       // mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
       // mgmt.makePropertyKey("values").dataType(Double.class).cardinality(Cardinality.LIST).make();

        PropertyKey hobbies = mgmt.makePropertyKey("hobbies").cardinality(Cardinality.LIST).dataType(String.class).make();

        mgmt.setTTL(hobbies, Duration.ofMinutes(5));
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
        VertexLabel personLabel = mgmt.makeVertexLabel("city").make();
        mgmt.commit();
    }
    @Test
    public void testBindMultiProperties2Vertex() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //获取现有属性定义
     //   PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
     //   PropertyKey name = mgmt.getPropertyKey("name");
        PropertyKey hobbies = mgmt.getPropertyKey("hobbies");
        //获取顶点标签
        VertexLabel personLabel = mgmt.getVertexLabel("person");
        //绑定标签属性
        mgmt.addProperties(personLabel,hobbies);

        mgmt.commit();
    }
    @Test
    public void testBindMultiProperties2Edge() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //获取现有属性定义
        PropertyKey birthDay = mgmt.getPropertyKey("name");
        //获取顶点标签
        EdgeLabel mother = mgmt.getEdgeLabel("visits");
        //绑定标签属性
        mgmt.addProperties(mother,birthDay);
        mgmt.commit();
    }@Test
    public void testAddConnectionConstraints() {
        JanusGraphManagement mgmt = janusGraph.openManagement();
        VertexLabel personLabel = mgmt.getVertexLabel("person");
        VertexLabel city = mgmt.getVertexLabel("city");
        EdgeLabel motherEdgeLabel = mgmt.getEdgeLabel("lives");

        mgmt.addConnection(motherEdgeLabel,personLabel,city);
        mgmt.commit();
    }

    @Test
    public void testCreateVertex(){
        VertexLabel personLabel = janusGraph.getVertexLabel("person");

        JanusGraphVertex p1 = janusGraph.addVertex(T.label,personLabel,"name","文玉");
        JanusGraphVertex p2 = janusGraph.addVertex(T.label,personLabel,"name","泽宇");

        GraphTraversalSource g = janusGraph.traversal();
        Vertex bj = g.V().hasLabel("city").has("name", "北京").next();

        p2.addEdge("mother",p1,"name","泽宇妈妈");
        p2.addEdge("lives",bj,"name","出生在北京");
        p1.addEdge("love",p2,"name","妈妈单边爱儿子");

        janusGraph.tx().commit();
    }
    @Test
    public void testCreateEdge(){
        EdgeLabel motherEdgeLabel = janusGraph.getEdgeLabel("mother");

        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex boy = traversal.V().has("name", "泽宇").next();
        Vertex mama = traversal.V().has("name", "文玉").next();
        boy.addEdge("mother",mama,"name","泽宇的妈妈");
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
        VertexLabel city = mgmt.getVertexLabel("city");
        EdgeLabel visits = mgmt.makeEdgeLabel("visits").multiplicity(Multiplicity.ONE2ONE).make();
        mgmt.addProperties(visits,mgmt.getPropertyKey("name"));

        mgmt.setTTL(visits, Duration.ofMinutes(5));

        mgmt.addConnection(visits,person,city);
        mgmt.commit();
    }
    @Test
    public void testEdgeTTL011(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        EdgeLabel visits = mgmt.getEdgeLabel("visits");
        visits.remove();
        mgmt.commit();
    }
    @Test
    public void testTTL02(){

        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex zeyu = traversal.V().has("name", "泽宇").next();
        Vertex beijing = traversal.V().has("name", "北京").next();
        zeyu.addEdge("visits",beijing,"name","泽宇出生在北京");

        traversal.tx().commit();
    }
    @Test
    public void testTTL04(){

        GraphTraversalSource traversal = janusGraph.traversal();
        Vertex zeyu = traversal.V().has("name", "泽宇").next();
        zeyu.property("award","好孩子");
        traversal.tx().commit();
    }

    @Test
    public void testTTLVertex01(){
        JanusGraphManagement mgmt = janusGraph.openManagement();

        //创建5分钟的TTL标签
        VertexLabel home = mgmt.makeVertexLabel("home").setStatic().make();
        mgmt.setTTL(home,Duration.ofMinutes(5));
        PropertyKey name = mgmt.getPropertyKey("name");
        mgmt.addProperties(home,name);

        //构建一条居住的边
        EdgeLabel lives = mgmt.makeEdgeLabel("lives").multiplicity(Multiplicity.ONE2MANY).make();
        mgmt.addProperties(lives,name);

        //获取person定点标签
        VertexLabel person = mgmt.getVertexLabel("person");

        //将person和home使用lives连接在一起
        mgmt.addConnection(lives,person,home);

        mgmt.commit();
    }
    @Test
    public void testTTLVertex02(){
        JanusGraphVertex home = janusGraph.addVertex(T.label, "home", "name", "回龙观新村");

        GraphTraversalSource g = janusGraph.traversal();
        Vertex zeyu = g.V().has("name", "泽宇").next();
        zeyu.addEdge("lives",home,"name","工作临时居住");

        g.tx().commit();
    }
    @Test
    public void testTTL03(){

        GraphTraversalSource traversal = janusGraph.traversal();
        GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = traversal.V().outE("visits").inV().valueMap("name");

        System.out.println(vertexMapGraphTraversal);
    }

    @Test
    public void testMultiProperties01(){

        GraphTraversalSource g = janusGraph.traversal();
        Vertex zeyu = g.V().has("name", "泽宇").next();
        VertexProperty<String> p1 = zeyu.property("hobbies", "汪汪队");
        p1.property("name","jiangzz");
        VertexProperty<String> p2 = zeyu.property("hobbies", "超级飞侠");
        p2.property("name","wangwy");

        Iterator<VertexProperty<Object>> hobbies = zeyu.properties("hobbies");
        while (hobbies.hasNext()) {
            VertexProperty<Object> next = hobbies.next();

            System.out.println(next.value()+" "+next.property("name"));
        }


        g.tx().commit();
    }
    @Test
    public void testUnidirectedEdges(){
        JanusGraphManagement mgmt = janusGraph.openManagement();
        //构建love边
        EdgeLabel edgeLabel = mgmt.makeEdgeLabel("love").unidirected().make();
        mgmt.setTTL(edgeLabel,Duration.ofMinutes(5));
        PropertyKey name = mgmt.getPropertyKey("name");
        mgmt.addProperties(edgeLabel,name);
        VertexLabel person = mgmt.getVertexLabel("person");
        mgmt.addConnection(edgeLabel,person,person);
        mgmt.commit();

        GraphTraversalSource g = janusGraph.traversal();
        Vertex p1 = g.V().has("name", "文玉").next();
        Vertex p2 = g.V().has("name", "泽宇").next();
        //添加单边关联
        p1.addEdge("love",p2,"name","妈妈爱儿子");

        g.tx().commit();
    }

    @Test
    public void testDropVertex(){

        GraphTraversalSource g = janusGraph.traversal();
        //Vertex p2 = g.V().has("name", "泽宇").next();
        Vertex next = g.V().has("name","文玉").outE("love").outV().out().next();
        next.remove();
        g.tx().commit();
    }


    @Test
    public void testQueryProperties(){
        JanusGraphManagement janusGraphManagement = janusGraph.openManagement();
        Iterable<VertexLabel> vertexLabels = janusGraphManagement.getVertexLabels();
        vertexLabels.forEach(vertexLabel -> {
            System.out.println("顶点:"+vertexLabel.name()+ " id:"+vertexLabel.id());
            Collection<PropertyKey> propertyKeys = vertexLabel.mappedProperties();
            for (PropertyKey propertyKey : propertyKeys) {
                System.out.println(propertyKey.name()+" "+propertyKey.cardinality()+" "+propertyKey.dataType());
            }
            System.out.println();
        });

    }

    @Test
    public void testDropSchema() throws BackendException {
        JanusGraphFactory.drop(janusGraph);
    }

    @After
    public void after(){
        janusGraph.close();
    }
}
