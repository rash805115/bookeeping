package bookeeping.backend.database.service.neo4jembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.database.neo4j.RelationshipLabels;
import bookeeping.backend.database.service.FilesystemService;
import bookeeping.backend.exception.DuplicateFilesystem;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;

public class FilesystemServiceImpl implements FilesystemService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public FilesystemServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewFilesystem(String commitId, String userId, String filesystemId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getFilesystem(userId, filesystemId);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
			{
				Node filesystem = this.commonCode.createNode(NodeLabels.Filesystem);
				filesystem.setProperty(MandatoryProperties.filesystemId.name(), filesystemId);
				filesystem.setProperty(MandatoryProperties.version.name(), 0);
				String filesystemNodeId = (String) filesystem.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> filesystemPropertiesEntry : filesystemProperties.entrySet())
				{
					filesystem.setProperty(filesystemPropertiesEntry.getKey(), filesystemPropertiesEntry.getValue());
				}
				
				Node rootDirectory = this.commonCode.createNode(NodeLabels.Directory);
				Node user = this.commonCode.getUser(userId);
				user.createRelationshipTo(filesystem, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				
				filesystem.createRelationshipTo(rootDirectory, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				transaction.success();
				return filesystemNodeId;
			}
		}
	}
	
	@Override
	public void restoreFilesystem(String commitId, String userId, String filesystemId, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DuplicateFilesystem, NodeNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getFilesystem(userId, filesystemId);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
			{
				this.commonCode.restoreNode(commitId, nodeIdToBeRestored);
				transaction.success();
			}
		}
	}

	@Override
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node filesystem = this.commonCode.getFilesystem(userId, filesystemId);
			Map<String, Object> filesystemProperties = null;
			try
			{
				filesystemProperties = this.commonCode.getNodeProperties(filesystem);
			}
			catch(NodeNotFound nodeNotFound) {}
			
			transaction.success();
			return filesystemProperties;
		}
	}
}
