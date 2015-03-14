package database.service.titancassandraembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import database.MandatoryProperties;
import database.connection.singleton.TitanCassandraEmbeddedConnection;
import database.service.FileService;
import database.titan.NodeLabels;
import database.titan.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class FileServiceImpl implements FileService
{
	private TitanGraph titanGraph;
	
	public FileServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public void createNewFile(String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex parentDirectory = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				if(filePath.equals("/"))
				{
					parentDirectory = commonCode.getRootDirectory(userId, filesystemId);
				}
				else
				{
					String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
					String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
					directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
					parentDirectory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				}
				
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Vertex node = this.titanGraph.addVertexWithLabel(NodeLabels.File.name());
				node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty(MandatoryProperties.filePath.name(), filePath);
				node.setProperty(MandatoryProperties.fileName.name(), fileName);
				node.setProperty(MandatoryProperties.version.name(), 0);
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					node.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.addEdge(RelationshipLabels.has.name(), node);
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
	public void createNewVersion(String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex file = null;
			try
			{
				file = commonCode.getVersion("file", userId, filesystemId, filePath, fileName, -1, false);
			}
			catch (VersionNotFound e) {}
			Vertex versionedFile = commonCode.copyNode(file);
			
			int fileLatestVersion = (int) file.getProperty(MandatoryProperties.version.name());
			versionedFile.setProperty(MandatoryProperties.version.name(), fileLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedFile.setProperty(entry.getKey(), entry.getValue());
			}
			
			Edge relationship = file.addEdge(RelationshipLabels.hasVersion.name(), versionedFile);
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
	public void deleteFileTemporarily(String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex file = new CommonCode().getFile(userId, filesystemId, filePath, fileName, false);
			Edge hasRelationship = file.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator().next();
			Vertex parentDirectory = hasRelationship.getVertex(Direction.OUT);
			
			Edge hadRelationship = parentDirectory.addEdge(RelationshipLabels.had.name(), file);
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
	public void restoreTemporaryDeletedFile(String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			try
			{
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Vertex file = commonCode.getFile(userId, filesystemId, filePath, fileName, true);
				Edge hadRelationship = file.getEdges(Direction.IN, RelationshipLabels.had.name()).iterator().next();
				Vertex parentDirectory = hadRelationship.getVertex(Direction.OUT);
				
				Edge hasRelationship = parentDirectory.addEdge(RelationshipLabels.has.name(), file);
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
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex file = new CommonCode().getVersion("file", userId, filesystemId, filePath, fileName, version, false);
			Map<String, Object> fileProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = file.getPropertyKeys();
			for(String key : keys)
			{
				fileProperties.put(key, file.getProperty(key));
			}
			
			titanTransaction.commit();
			return fileProperties;
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
