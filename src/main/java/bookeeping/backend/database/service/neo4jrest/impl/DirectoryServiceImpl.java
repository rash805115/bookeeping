package bookeeping.backend.database.service.neo4jrest.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JRestConnection;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.database.neo4j.RelationshipLabels;
import bookeeping.backend.database.service.DirectoryService;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public class DirectoryServiceImpl implements DirectoryService
{
	private Neo4JRestConnection neo4jRestConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public DirectoryServiceImpl()
	{
		this.neo4jRestConnection = Neo4JRestConnection.getInstance();
		this.graphDatabaseService = this.neo4jRestConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, VersionNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Node directory = this.commonCode.createNode(NodeLabels.Directory);
				directory.setProperty(MandatoryProperties.directoryPath.name(), directoryPath);
				directory.setProperty(MandatoryProperties.directoryName.name(), directoryName);
				directory.setProperty(MandatoryProperties.version.name(), 0);
				String directoryNodeId = (String) directory.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
				{
					directory.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
				}
				
				Node rootDirectory = this.commonCode.getRootDirectory(userId, filesystemId, filesystemVersion);
				rootDirectory.createRelationshipTo(directory, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				transaction.success();
				return directoryNodeId;
			}
		}
	}
	
	@Override
	public void restoreDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateDirectory, NodeNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				this.commonCode.restoreNode(commitId, nodeIdToBeRestored);
				transaction.success();
			}
		}
	}
	
	@Override
	public String moveDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String oldDirectoryPath, String oldDirectoryName, String newDirectoryPath, String newDirectoryName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			List<String> ignoreRelationships = new ArrayList<String>();
			ignoreRelationships.add(RelationshipLabels.hadAccess.name());
			ignoreRelationships.add(RelationshipLabels.hasAccess.name());
			ignoreRelationships.add(RelationshipLabels.hasVersion.name());
			
			Node oldDirectory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, oldDirectoryPath, oldDirectoryName);
			Node newDirectory = this.commonCode.copyNodeTree(oldDirectory, ignoreRelationships);
			newDirectory.setProperty(MandatoryProperties.directoryPath.name(), newDirectoryPath);
			newDirectory.setProperty(MandatoryProperties.directoryName.name(), newDirectoryName);
			newDirectory.setProperty(MandatoryProperties.version.name(), 0);
			String newDirectoryNodeId = (String) newDirectory.getProperty(MandatoryProperties.nodeId.name());
			
			Node parentNode = oldDirectory.getSingleRelationship(RelationshipLabels.has, Direction.INCOMING).getStartNode();
			parentNode.createRelationshipTo(newDirectory, RelationshipLabels.has);
			
			GenericService genericService = new GenericServiceImpl();
			try
			{
				genericService.deleteNodeTemporarily(commitId, (String) oldDirectory.getProperty(MandatoryProperties.nodeId.name()));
			}
			catch (NodeNotFound | NodeUnavailable e) {}
			
			String oldPath = oldDirectoryPath.equals("/") ? "/" + oldDirectoryName : oldDirectoryPath + "/" + oldDirectoryName;
			String newPath = newDirectoryPath.equals("/") ? "/" + newDirectoryName : newDirectoryPath + "/" + newDirectoryName;
			
			for(Relationship newRelationship : newDirectory.getRelationships(Direction.OUTGOING))
			{
				Node endNode = newRelationship.getEndNode();
				endNode.setProperty(MandatoryProperties.filePath.name(), newPath);
			}
			
			List<Node> directoryList = this.commonCode.getAllDirectory(userId, filesystemId, filesystemVersion);
			for(Node directory : directoryList)
			{
				String directoryPath = (String) directory.getProperty(MandatoryProperties.directoryPath.name());
				if(directoryPath.startsWith(oldPath))
				{
					directory.setProperty(MandatoryProperties.directoryPath.name(), directoryPath.replaceFirst(oldPath, newPath));
					for(Relationship newRelationship : directory.getRelationships(Direction.OUTGOING))
					{
						Node endNode = newRelationship.getEndNode();
						String filePath = (String) endNode.getProperty(MandatoryProperties.filePath.name());
						if(filePath != null) endNode.setProperty(MandatoryProperties.filePath.name(), filePath.replaceFirst(oldPath, newPath));
					}
				}
			}
			
			transaction.success();
			return newDirectoryNodeId;
		}
	}
	
	@Override
	public Map<String, Object> getDirectory(String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node directory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
			Map<String, Object> directoryProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = directory.getPropertyKeys();
			for(String key : keys)
			{
				directoryProperties.put(key, directory.getProperty(key));
			}
			
			transaction.success();
			return directoryProperties;
		}
	}
}
