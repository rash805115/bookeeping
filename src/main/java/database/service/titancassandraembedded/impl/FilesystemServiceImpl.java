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
import database.service.FilesystemService;
import database.titan.NodeLabels;
import database.titan.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.DuplicateFilesystem;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class FilesystemServiceImpl implements FilesystemService
{
	private TitanGraph titanGraph;
	
	public FilesystemServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex user = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				user = commonCode.getUser(userId);
				commonCode.getFilesystem(userId, filesystemId, false);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
			{
				AutoIncrementServiceImpl autoIncrementServiceImpl = new AutoIncrementServiceImpl();
				
				Vertex node = this.titanGraph.addVertexWithLabel(NodeLabels.Filesystem.name());
				node.setProperty("nodeId", autoIncrementServiceImpl.getNextAutoIncrement());
				node.setProperty("filesystemId", filesystemId);
				node.setProperty("version", 0);
				
				for(Entry<String, Object> filesystemPropertiesEntry : filesystemProperties.entrySet())
				{
					node.setProperty(filesystemPropertiesEntry.getKey(), filesystemPropertiesEntry.getValue());
				}
				
				Vertex rootDirectory = this.titanGraph.addVertexWithLabel(NodeLabels.Directory.name());
				rootDirectory.setProperty("nodeId", autoIncrementServiceImpl.getNextAutoIncrement());
				
				user.addEdge(RelationshipLabels.has.name(), node);
				node.addEdge(RelationshipLabels.has.name(), rootDirectory);
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
	public void createNewVersion(String userId, String filesystemId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex filesystem = null;
			try
			{
				filesystem = commonCode.getVersion("filesystem", userId, filesystemId, null, null, -1);
			}
			catch (VersionNotFound | FileNotFound | DirectoryNotFound e) {}
			Vertex versionedFilesystem = commonCode.copyNodeTree(filesystem);
			
			int filesystemLatestVersion = (int) filesystem.getProperty("version");
			versionedFilesystem.setProperty("version", filesystemLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedFilesystem.setProperty(entry.getKey(), entry.getValue());
			}
			
			Edge relationship = filesystem.addEdge(RelationshipLabels.hasVersion.name(), versionedFilesystem);
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
	public void deleteFilesystemTemporarily(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex filesystem = new CommonCode().getFilesystem(userId, filesystemId, false);
			Edge hasRelationship = filesystem.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator().next();
			Vertex parentDirectory = hasRelationship.getVertex(Direction.OUT);
			
			Edge hadRelationship = parentDirectory.addEdge(RelationshipLabels.had.name(), filesystem);
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
	public void restoreTemporaryDeletedFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex filesystem = new CommonCode().getFilesystem(userId, filesystemId, false);
			Edge hadRelationship = filesystem.getEdges(Direction.IN, RelationshipLabels.had.name()).iterator().next();
			Vertex parentDirectory = hadRelationship.getVertex(Direction.OUT);
			
			Edge hasRelationship = parentDirectory.addEdge(RelationshipLabels.has.name(), filesystem);
			for(String key : hadRelationship.getPropertyKeys())
			{
				hasRelationship.setProperty(key, hadRelationship.getProperty(key));
			}
			
			hadRelationship.remove();
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
	public Map<String, Object> getFilesystem(String userId, String filesystemId, int version) throws UserNotFound, FilesystemNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex filesystem = null;
			try
			{
				filesystem = new CommonCode().getVersion("filesystem", userId, filesystemId, null, null, version);
			}
			catch (FileNotFound | DirectoryNotFound e) {}
			Map<String, Object> filesystemProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = filesystem.getPropertyKeys();
			for(String key : keys)
			{
				filesystemProperties.put(key, filesystem.getProperty(key));
			}
			
			titanTransaction.commit();
			return filesystemProperties;
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
