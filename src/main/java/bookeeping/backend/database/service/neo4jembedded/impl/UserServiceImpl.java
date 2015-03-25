package bookeeping.backend.database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.database.service.UserService;
import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.UserNotFound;

public class UserServiceImpl implements UserService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public UserServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getUser(userId);
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			catch(UserNotFound userNotFound)
			{
				Node node = this.commonCode.createNode(NodeLabels.User);
				node.setProperty(MandatoryProperties.userId.name(), userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					node.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
				}
				
				transaction.success();
			}
		}
	}
	
	@Override
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = this.commonCode.getUser(userId);
			Map<String, Object> userProperties = new HashMap<String, Object>();
			
			Iterable<String> iterable = user.getPropertyKeys();
			for(String key : iterable)
			{
				userProperties.put(key, user.getProperty(key));
			}
			
			transaction.success();
			return userProperties;
		}
	}
}
