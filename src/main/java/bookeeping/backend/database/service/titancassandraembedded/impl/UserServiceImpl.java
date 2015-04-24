package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.UserService;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.exception.DuplicateUser;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.UserNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class UserServiceImpl implements UserService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public UserServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			try
			{
				this.commonCode.getUser(userId);
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			catch(UserNotFound userNotFound)
			{
				Vertex node = this.commonCode.createNode(NodeLabels.User);
				node.setProperty(MandatoryProperties.userId.name(), userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					node.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
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
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex user = this.commonCode.getUser(userId);
			Map<String, Object> userProperties = null;
			try
			{
				userProperties = this.commonCode.getNodeProperties(user);
			}
			catch(NodeNotFound nodeNotFound) {}
			
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
}
