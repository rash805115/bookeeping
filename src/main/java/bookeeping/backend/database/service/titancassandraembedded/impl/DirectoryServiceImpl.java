package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.DirectoryService;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.database.titan.RelationshipLabels;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateDirectory;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class DirectoryServiceImpl implements DirectoryService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public DirectoryServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, VersionNotFound, DuplicateDirectory
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			try
			{
				this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Vertex directory = this.commonCode.createNode(NodeLabels.Directory);
				directory.setProperty(MandatoryProperties.directoryPath.name(), directoryPath);
				directory.setProperty(MandatoryProperties.directoryName.name(), directoryName);
				directory.setProperty(MandatoryProperties.version.name(), 0);
				String directoryNodeId = (String) directory.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
				{
					directory.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
				}
				
				Vertex rootDirectory = this.commonCode.getRootDirectory(userId, filesystemId, filesystemVersion);
				rootDirectory.addEdge(RelationshipLabels.has.name(), directory).setProperty(MandatoryProperties.commitId.name(), commitId);
				titanTransaction.commit();
				return directoryNodeId;
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
	public void restoreDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateDirectory, NodeNotFound, NodeUnavailable
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			try
			{
				this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				this.commonCode.restoreNode(commitId, nodeIdToBeRestored);
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
	public String moveDirectory(String commitId, String userId, String filesystemId, int filesystemVersion, String oldDirectoryPath, String oldDirectoryName, String newDirectoryPath, String newDirectoryName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound, DuplicateDirectory
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			List<String> ignoreRelationships = new ArrayList<String>();
			ignoreRelationships.add(RelationshipLabels.hadAccess.name());
			ignoreRelationships.add(RelationshipLabels.hasAccess.name());
			ignoreRelationships.add(RelationshipLabels.hasVersion.name());
			
			Vertex oldDirectory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, oldDirectoryPath, oldDirectoryName);
			Vertex newDirectory = this.commonCode.copyNodeTree(oldDirectory, ignoreRelationships);
			newDirectory.setProperty(MandatoryProperties.directoryPath.name(), newDirectoryPath);
			newDirectory.setProperty(MandatoryProperties.directoryName.name(), newDirectoryName);
			newDirectory.setProperty(MandatoryProperties.version.name(), 0);
			String newDirectoryNodeId = (String) newDirectory.getProperty(MandatoryProperties.nodeId.name());
			
			Vertex parentNode = oldDirectory.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator().next().getVertex(Direction.OUT);
			parentNode.addEdge(RelationshipLabels.has.name(), newDirectory);
			
			GenericService genericService = new GenericServiceImpl();
			try
			{
				genericService.deleteNodeTemporarily(commitId, (String) oldDirectory.getProperty(MandatoryProperties.nodeId.name()));
			}
			catch (NodeNotFound | NodeUnavailable e) {}
			
			String oldPath = oldDirectoryPath.equals("/") ? "/" + oldDirectoryName : oldDirectoryPath + "/" + oldDirectoryName;
			String newPath = newDirectoryPath.equals("/") ? "/" + newDirectoryName : newDirectoryPath + "/" + newDirectoryName;
			
			for(RelationshipLabels relationshipLabel : RelationshipLabels.values())
			{
				for(Edge newRelationship : newDirectory.getEdges(Direction.OUT, relationshipLabel.name()))
				{
					Vertex endNode = newRelationship.getVertex(Direction.IN);
					endNode.setProperty(MandatoryProperties.filePath.name(), newPath);
				}
			}
			
			List<Vertex> directoryList = this.commonCode.getAllDirectory(userId, filesystemId, filesystemVersion);
			for(Vertex directory : directoryList)
			{
				String directoryPath = (String) directory.getProperty(MandatoryProperties.directoryPath.name());
				if(directoryPath.startsWith(oldPath))
				{
					directory.setProperty(MandatoryProperties.directoryPath.name(), directoryPath.replaceFirst(oldPath, newPath));
					for(RelationshipLabels relationshipLabel : RelationshipLabels.values())
					{
						for(Edge newRelationship : newDirectory.getEdges(Direction.OUT, relationshipLabel.name()))
						{
							Vertex endNode = newRelationship.getVertex(Direction.IN);
							String filePath = (String) endNode.getProperty(MandatoryProperties.filePath.name());
							if(filePath != null) endNode.setProperty(MandatoryProperties.filePath.name(), filePath.replaceFirst(oldPath, newPath));
						}
					}
				}
			}
			
			titanTransaction.commit();
			return newDirectoryNodeId;
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
	public Map<String, Object> getDirectory(String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex directory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
			Map<String, Object> directoryProperties = null;
			try
			{
				directoryProperties = this.commonCode.getNodeProperties(directory);
			}
			catch(NodeNotFound nodeNotFound) {}
			
			titanTransaction.commit();
			return directoryProperties;
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
