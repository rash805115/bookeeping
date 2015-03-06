package database.service.neo4jembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import database.service.FilesystemService;
import exception.DuplicateFilesystem;
import exception.UserNotFound;

public class FilesystemServiceImpl implements FilesystemService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public FilesystemServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}
	
	@Override
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			Node user = readableIndex.get("userId", userId).getSingle();
			
			if(user == null)
			{
				transaction.success();
				throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
			}
			else
			{
				Iterable<Relationship> iterable = user.getRelationships(RelationshipLabels.owns_belongsTo);
				for(Relationship relationship : iterable)
				{
					Node filesystem = relationship.getEndNode();
					String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
					
					if(retrievedFilesystemId.equals(filesystemId))
					{
						transaction.success();
						throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
					}
				}
				
				Node node = this.graphDatabaseService.createNode(NodeLabels.Filesystem);
				node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty("filesystemId", filesystemId);
				
				for(Entry<String, Object> filesystemPropertiesEntry : filesystemProperties.entrySet())
				{
					node.setProperty(filesystemPropertiesEntry.getKey(), filesystemPropertiesEntry.getValue());
				}
				
				user.createRelationshipTo(node, RelationshipLabels.owns_belongsTo);
				
				transaction.success();
			}
		}
	}
}
