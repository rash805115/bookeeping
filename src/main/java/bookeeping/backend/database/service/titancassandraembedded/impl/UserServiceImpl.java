package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.UserService;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.UserNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class UserServiceImpl implements UserService
{
	private TitanGraph titanGraph;
	
	public UserServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			try
			{
				new CommonCode().getUser(userId);
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			catch(UserNotFound userNotFound)
			{
				Vertex vertex = this.titanGraph.addVertexWithLabel(NodeLabels.User.name());
				vertex.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				vertex.setProperty(MandatoryProperties.userId.name(), userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					vertex.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
				}
				
				titanTransaction.commit();
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public int countUsers()
	{
		return 0;
	}

	@Override
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex user = new CommonCode().getUser(userId);
			Map<String, Object> userProperties = new HashMap<String, Object>();
			
			Iterable<String> iterable = user.getPropertyKeys();
			for(String key : iterable)
			{
				userProperties.put(key, user.getProperty(key));
			}
			
			titanTransaction.commit();
			return userProperties;
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public List<Map<String, Object>> getUsersByMatchingAllProperty(Map<String, Object> userProperties)
	{
		return null;
	}
	
	@Override
	public List<Map<String, Object>> getUsersByMatchingAnyProperty(Map<String, Object> userProperties)
	{
		return null;
	}
}
