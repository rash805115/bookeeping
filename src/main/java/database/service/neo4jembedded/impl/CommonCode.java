package database.service.neo4jembedded.impl;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.ReadableIndex;

import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class CommonCode
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public CommonCode()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}
	
	public Node getUser(String userId) throws UserNotFound
	{
		ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
		Node user = readableIndex.get("userId", userId).getSingle();
		
		if(user == null)
		{
			throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
		}
		else
		{
			return user;
		}
	}
	
	public Node getFilesystem(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Node user  = this.getUser(userId);
		Iterable<Relationship> iterable = user.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		for(Relationship relationship : iterable)
		{
			Node filesystem = relationship.getEndNode();
			String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
			
			if(retrievedFilesystemId.equals(filesystemId))
			{
				return filesystem;
			}
		}
		
		throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
	}
	
	public Node getRootDirectory(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Node filesystem = this.getFilesystem(userId, filesystemId);
		return filesystem.getRelationships(Direction.OUTGOING, RelationshipLabels.has).iterator().next().getEndNode();
	}
	
	public Node getDirectory(String userId, String filesystemId, String directoryPath, String directoryName) throws FilesystemNotFound, UserNotFound, DirectoryNotFound
	{
		Node rootDirectory = this.getRootDirectory(userId, filesystemId);
		Iterable<Relationship> iterable = rootDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.Directory))
			{
				String retrievedDirectoryPath = (String) node.getProperty("directoryPath");
				String retrievedDirectoryName = (String) node.getProperty("directoryName");
				
				if(retrievedDirectoryPath.equals(directoryPath) && retrievedDirectoryName.equals(directoryName))
				{
					return node;
				}
			}
		}
		
		throw new DirectoryNotFound("ERROR: Directory not found! - \"" + directoryPath + "/" + directoryName + "\"");
	}
	
	public Node getFile(String userId, String filesystemId, String filePath, String fileName) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound
	{
		Node parentDirectory = null;
		if(filePath.equals("/"))
		{
			parentDirectory = this.getRootDirectory(userId, filesystemId);
		}
		else
		{
			String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
			String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
			directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
			parentDirectory = this.getDirectory(userId, filesystemId, directoryPath, directoryName);
		}
		
		Iterable<Relationship> iterable = parentDirectory.getRelationships(RelationshipLabels.has);
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.File))
			{
				String retrievedFilePath = (String) node.getProperty("filePath");
				String retrievedFileName = (String) node.getProperty("fileName");
				
				if(retrievedFilePath.equals(filePath) && retrievedFileName.equals(fileName))
				{
					return node;
				}
			}
		}
		
		throw new FileNotFound("ERROR: File not found! - \"" + filePath + "/" + fileName + "\"");
	}
	
	public Node getVersion(String nodeType, String userId, String filesystemId, String path, String name, int version) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		Node node = null;
		if(nodeType.equalsIgnoreCase("filesystem"))
		{
			node = this.getFilesystem(userId, filesystemId);
		}
		else if(nodeType.equalsIgnoreCase("directory"))
		{
			node = this.getDirectory(userId, filesystemId, path, name);
		}
		else
		{
			node = this.getFile(userId, filesystemId, path, name);
		}
		
		do
		{
			if(version != -1)
			{
				int retrievedVersion = (int) node.getProperty("version");
				if(retrievedVersion == version)
				{
					return node;
				}
			}
			
			Iterator<Relationship> iterator = node.getRelationships(Direction.OUTGOING, RelationshipLabels.hasVersion).iterator();
			if(iterator.hasNext())
			{
				node = iterator.next().getEndNode();
			}
			else
			{
				if(version == -1)
				{
					return node;
				}
				else
				{
					node = null;
				}
			}
		}
		while(node != null);
		
		if(nodeType.equalsIgnoreCase("filesystem"))
		{
			throw new VersionNotFound("ERROR: Version not found! - Filesystem: \"" + filesystemId + "\", Version - \"" + version + "\"");
		}
		else if(nodeType.equalsIgnoreCase("directory"))
		{
			throw new VersionNotFound("ERROR: Version not found! - Directory: \"" + path + "/" + name + "\", Version - \"" + version + "\"");
		}
		else
		{
			throw new VersionNotFound("ERROR: Version not found! - File: \"" + path + "/" + name + "\", Version - \"" + version + "\"");
		}
	}
	
	public Node copyNode(Node node)
	{
		Node copyNode = this.graphDatabaseService.createNode(node.getLabels().iterator().next());
		for(String key : node.getPropertyKeys())
		{
			copyNode.setProperty(key, node.getProperty(key));
		}
		
		return copyNode;
	}
}
