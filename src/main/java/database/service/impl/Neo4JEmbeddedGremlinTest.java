package database.service.impl;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.graphdb.RestGraphDatabase;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class Neo4JEmbeddedGremlinTest
{
	public static void main(String args[])
	{
		GraphDatabaseService graphDatabaseService = new RestGraphDatabase("http://10.20.230.12:7474/data/graph.db/");
		Graph graph = new Neo4jGraph(graphDatabaseService);
		
		Vertex rash = graph.addVertex(null);
		rash.setProperty("userId", 1);
		rash.setProperty("username", "rash");
		
		Vertex honey = graph.addVertex(null);
		honey.setProperty("userId", 2);
		honey.setProperty("username", "honey");
		
		Edge loveEdge = graph.addEdge(null, rash, honey, "LOVES");
		loveEdge.setProperty("since", 2011);
		
		GremlinPipeline<Vertex, String> gremlinPipeline = new GremlinPipeline<Vertex, String>();
		Iterator<Object> results = gremlinPipeline.start(rash).out("LOVES").property("username").iterator();
		
		while(results.hasNext())
		{
			System.out.println(results.next());
		}
	}
}
