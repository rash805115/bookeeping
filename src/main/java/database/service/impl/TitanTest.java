package database.service.impl;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class TitanTest
{
	public static void main(String args[])
	{
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		baseConfiguration.setProperty("storage.backend", "cassandra");
		baseConfiguration.setProperty("storage.hostname", "10.20.230.12");
		
		TitanGraph titanGraph = TitanFactory.open(baseConfiguration);
		
		Vertex rash = titanGraph.addVertex(null);
		rash.setProperty("userId", 1);
		rash.setProperty("username", "rash");
		
		Vertex honey = titanGraph.addVertex(null);
		honey.setProperty("userId", 2);
		honey.setProperty("username", "honey");
		
		Edge loves = titanGraph.addEdge(null, rash, honey, "LOVES");
		loves.setProperty("since", 2011);
		
		Iterable<Vertex> results = rash.query().labels("LOVES").has("since", 2011).vertices();
		
		for(Vertex vertex : results)
		{
			System.out.println("User Id: " + vertex.getProperty("userId"));
			System.out.println("Username: " + vertex.getProperty("username"));
			System.out.println("-----------------------");
		}
		
		results = titanGraph.getVertices("userId", 1);
		
		for(Vertex vertex : results)
		{
			System.out.println("User Id: " + vertex.getProperty("userId"));
			System.out.println("Username: " + vertex.getProperty("username"));
			System.out.println("-----------------------");
		}
		
		System.out.println(new GremlinPipeline<Vertex, String>(titanGraph.getVertex(1)).out("LOVES").has("since", 2011).property("username"));
	}
}
