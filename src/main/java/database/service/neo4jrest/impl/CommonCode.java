package database.service.neo4jrest.impl;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.ReadableIndex;

import database.MandatoryProperties;
import database.connection.singleton.Neo4JRestConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class CommonCode
{
	private Neo4JRestConnection neo4jRestConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public CommonCode()
	{
		this.neo4jRestConnection = Neo4JRestConnection.getInstance();
		this.graphDatabaseService = this.neo4jRestConnection.getGraphDatabaseServiceObject();
	}
	
	public Node getUser(String userId) throws UserNotFound
	{
		ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
		Node user = readableIndex.get(MandatoryProperties.userId.name(), userId).getSingle();
		
		if(user == null)
		{
			throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
		}
		else
		{
			return user;
		}
	}
	
	public Node getFilesystem(String userId, String filesystemId, boolean deleted) throws FilesystemNotFound, UserNotFound
	{
		Node user  = this.getUser(userId);
		Iterable<Relationship> iterable;
		if(deleted)
		{
			iterable = user.getRelationships(Direction.OUTGOING, RelationshipLabels.had);
		}
		else
		{
			iterable = user.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		}
		
		for(Relationship relationship : iterable)
		{
			Node filesystem = relationship.getEndNode();
			String retrievedFilesystemId = (String) filesystem.getProperty(MandatoryProperties.filesystemId.name());
			
			if(retrievedFilesystemId.equals(filesystemId))
			{
				return filesystem;
			}
		}
		
		throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
	}
	
	public Node getRootDirectory(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Node filesystem = this.getFilesystem(userId, filesystemId, false);
		return filesystem.getSingleRelationship(RelationshipLabels.has, Direction.OUTGOING).getEndNode();
	}
	
	public Node getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound
	{
		Node rootDirectory = this.getRootDirectory(userId, filesystemId);
		Iterable<Relationship> iterable;
		if(deleted)
		{
			iterable = rootDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.had);
		}
		else
		{
			iterable = rootDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		}
		
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.Directory))
			{
				String retrievedDirectoryPath = (String) node.getProperty(MandatoryProperties.directoryPath.name());
				String retrievedDirectoryName = (String) node.getProperty(MandatoryProperties.directoryName.name());
				String retrievedCommitId = (String) node.getProperty(MandatoryProperties.commitId.name());
				
				if(retrievedDirectoryPath.equals(directoryPath) && retrievedDirectoryName.equals(directoryName) && (commitId == null ? true : retrievedCommitId.equals(commitId)))
				{
					return node;
				}
			}
		}
		
		throw new DirectoryNotFound("ERROR: Directory not found! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
	}
	
	public Node getFile(String userId, String filesystemId, String filePath, String fileName, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound
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
			parentDirectory = this.getDirectory(userId, filesystemId, directoryPath, directoryName, false, null);
		}
		
		Iterable<Relationship> iterable;
		if(deleted)
		{
			iterable = parentDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.had);
		}
		else
		{
			iterable = parentDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		}
		
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.File))
			{
				String retrievedFilePath = (String) node.getProperty(MandatoryProperties.filePath.name());
				String retrievedFileName = (String) node.getProperty(MandatoryProperties.fileName.name());
				String retrievedCommitId = (String) node.getProperty(MandatoryProperties.commitId.name());
				
				if(retrievedFilePath.equals(filePath) && retrievedFileName.equals(fileName) && (commitId == null ? true : retrievedCommitId.equals(commitId)))
				{
					return node;
				}
			}
		}
		
		throw new FileNotFound("ERROR: File not found! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
	}
	
	public Node getVersion(String nodeType, String userId, String filesystemId, String path, String name, int version, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		Node node = null;
		if(nodeType.equalsIgnoreCase("filesystem"))
		{
			node = this.getFilesystem(userId, filesystemId, deleted);
		}
		else if(nodeType.equalsIgnoreCase("directory"))
		{
			node = this.getDirectory(userId, filesystemId, path, name, deleted, commitId);
		}
		else
		{
			node = this.getFile(userId, filesystemId, path, name, deleted, commitId);
		}
		
		do
		{
			if(version != -1)
			{
				int retrievedVersion = (int) node.getProperty(MandatoryProperties.version.name());
				if(retrievedVersion == version)
				{
					return node;
				}
			}
			
			Relationship hasVersionRelationship = node.getSingleRelationship(RelationshipLabels.hasVersion, Direction.OUTGOING);
			if(hasVersionRelationship == null)
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
			else
			{
				node = hasVersionRelationship.getEndNode();
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
		Node copyNode = this.graphDatabaseService.createNode();
		for(Label label : node.getLabels())
		{
			copyNode.addLabel(label);
		}
		
		for(String key : node.getPropertyKeys())
		{
			if(! key.equals(MandatoryProperties.nodeId.name()))
			{
				copyNode.setProperty(key, node.getProperty(key));
			}
		}
		copyNode.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
		
		return copyNode;
	}
	
	public Node copyNodeTree(Node node)
	{
		List<Node> pendingNodeList = new ArrayList<Node>();
		pendingNodeList.add(node);
		
		List<Node> pendingNodeCopyList = new ArrayList<Node>();
		Node rootNodeCopy = this.copyNode(node);
		pendingNodeCopyList.add(rootNodeCopy);
		
		do
		{
			List<Node> childNodeList = new ArrayList<Node>();
			List<Node> childNodeCopyList = new ArrayList<Node>();
			for(int i = 0; i < pendingNodeList.size(); i++)
			{
				Node currentNode = pendingNodeList.get(i);
				Node currentNodeCopy = pendingNodeCopyList.get(i);
				
				Iterable<Relationship> currentNodeRelationships = currentNode.getRelationships(Direction.OUTGOING);
				for(Relationship relationship : currentNodeRelationships)
				{
					Node childNode = relationship.getEndNode();
					Node childNodeCopy = this.copyNode(childNode);
					Relationship currentNodeCopyRelationship = currentNodeCopy.createRelationshipTo(childNodeCopy, relationship.getType());
					
					for(String key : relationship.getPropertyKeys())
					{
						currentNodeCopyRelationship.setProperty(key, relationship.getProperty(key));
					}
					
					childNodeList.add(childNode);
					childNodeCopyList.add(childNodeCopy);
				}
			}
			
			pendingNodeList.clear();
			pendingNodeList.addAll(childNodeList);
			pendingNodeCopyList.clear();
			pendingNodeCopyList.addAll(childNodeCopyList);
		}
		while(! pendingNodeList.isEmpty());
		
		return rootNodeCopy;
	}
}
