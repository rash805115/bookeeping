package database.service.titancassandraembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import database.connection.singleton.TitanCassandraEmbeddedConnection;
import database.service.DirectoryService;
import database.titan.NodeLabels;
import database.titan.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class DirectoryServiceImpl implements DirectoryService
{
	private TitanGraph titanGraph;
	
	public DirectoryServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public void createNewDirectory(String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex rootDirectory = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				rootDirectory = commonCode.getRootDirectory(userId, filesystemId);
				commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Vertex node = this.titanGraph.addVertexWithLabel(NodeLabels.Directory.name());
				node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty("directoryPath", directoryPath);
				node.setProperty("directoryName", directoryName);
				node.setProperty("version", 0);
				
				for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
				{
					node.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
				}
				
				rootDirectory.addEdge(RelationshipLabels.has.name(), node);
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
	public void createNewVersion(String userId, String filesystemId, String directoryPath, String directoryName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex directory = null;
			try
			{
				directory = commonCode.getVersion("directory", userId, filesystemId, directoryPath, directoryName, -1, false);
			}
			catch (VersionNotFound | FileNotFound e) {}
			Vertex versionedDirectory = commonCode.copyNode(directory);
			
			int directoryLatestVersion = (int) directory.getProperty("version");
			versionedDirectory.setProperty("version", directoryLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedDirectory.setProperty(entry.getKey(), entry.getValue());
			}
			
			Edge relationship = directory.addEdge(RelationshipLabels.hasVersion.name(), versionedDirectory);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			
			titanTransaction.commit();
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
	public void deleteDirectoryTemporarily(String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex directory = new CommonCode().getDirectory(userId, filesystemId, directoryPath, directoryName, false);
			Edge hasRelationship = directory.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator().next();
			Vertex parentDirectory = hasRelationship.getVertex(Direction.OUT);
			
			Edge hadRelationship = parentDirectory.addEdge(RelationshipLabels.had.name(), directory);
			for(String key : hasRelationship.getPropertyKeys())
			{
				hadRelationship.setProperty(key, hasRelationship.getProperty(key));
			}
			
			hasRelationship.remove();
			titanTransaction.commit();
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
	public void restoreTemporaryDeletedDirectory(String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateDirectory
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			try
			{
				commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Vertex directory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, true);
				Edge hadRelationship = directory.getEdges(Direction.IN, RelationshipLabels.had.name()).iterator().next();
				Vertex parentDirectory = hadRelationship.getVertex(Direction.OUT);
				
				Edge hasRelationship = parentDirectory.addEdge(RelationshipLabels.has.name(), directory);
				for(String key : hadRelationship.getPropertyKeys())
				{
					hasRelationship.setProperty(key, hadRelationship.getProperty(key));
				}
				
				hadRelationship.remove();
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
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex directory = null;
			try
			{
				directory = new CommonCode().getVersion("directory", userId, filesystemId, directoryPath, directoryName, version, false);
			}
			catch (FileNotFound e) {}
			Map<String, Object> directoryProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = directory.getPropertyKeys();
			for(String key : keys)
			{
				directoryProperties.put(key, directory.getProperty(key));
			}
			
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
