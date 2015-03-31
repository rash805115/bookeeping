package bookeeping.backend.database.service.neo4jrest.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.ReadableIndex;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JRestConnection;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.database.neo4j.RelationshipLabels;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public class CommonCode
{
	private Neo4JRestConnection neo4jRestConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public CommonCode()
	{
		this.neo4jRestConnection = Neo4JRestConnection.getInstance();
		this.graphDatabaseService = this.neo4jRestConnection.getGraphDatabaseServiceObject();
	}
	
	public Node createNode(NodeLabels nodeLabel)
	{
		Node node = this.graphDatabaseService.createNode(nodeLabel);
		node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
		return node;
	}
	
	public Node getNode(String nodeId) throws NodeNotFound
	{
		ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
		Node node = readableIndex.get(MandatoryProperties.nodeId.name(), nodeId).getSingle();
		
		if(node == null)
		{
			throw new NodeNotFound("ERROR: Node not found! - \"" + nodeId + "\"");
		}
		else
		{
			return node;
		}
	}
	
	public Node createNodeVersion(String commidId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound, NodeUnavailable
	{
		Node node = null;
		try
		{
			node = this.getNodeVersion(nodeId, -1);
		}
		catch (VersionNotFound e) {}
		Node versionedNode = this.copyNodeTree(node, new ArrayList<String>());
		
		int nodeVersion = (int) node.getProperty(MandatoryProperties.version.name());
		versionedNode.setProperty(MandatoryProperties.version.name(), nodeVersion + 1);
		for(Entry<String, Object> entry : changedProperties.entrySet())
		{
			versionedNode.setProperty(entry.getKey(), entry.getValue());
		}
		
		Relationship relationship = node.createRelationshipTo(versionedNode, RelationshipLabels.hasVersion);
		for(Entry<String, Object> entry : changeMetadata.entrySet())
		{
			relationship.setProperty(entry.getKey(), entry.getValue());
		}
		relationship.setProperty(MandatoryProperties.commitId.name(), commidId);
		
		return versionedNode;
	}
	
	public Node getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound, NodeUnavailable
	{
		Node node = this.getNode(nodeId);
		do
		{
			int nodeVersion = -1;
			try
			{
				nodeVersion = (int) node.getProperty(MandatoryProperties.version.name());
			}
			catch(NotFoundException notFoundException)
			{
				throw new NodeUnavailable("ERROR: No version property for this node! - \"" + nodeId + "(v=" + version + ")\"");
			}
			
			if(nodeVersion == version)
			{
				return node;
			}
			else
			{
				Relationship relationship = node.getSingleRelationship(RelationshipLabels.hasVersion, Direction.OUTGOING);
				if(relationship != null)
				{
					node = relationship.getEndNode();
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
		}
		while(node != null);
		
		throw new VersionNotFound("ERROR: Node version not found! - \"" + nodeId + "(v=" + version + ")\"");
	}
	
	public Node deleteNodeTemporarily(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		Node node = this.getNode(nodeId);
		Relationship hasRelationship = node.getSingleRelationship(RelationshipLabels.has, Direction.INCOMING);
		if(hasRelationship == null)
		{
			throw new NodeUnavailable("ERROR: Node is unavailable! - \"" + nodeId + "\". Could be it has already been deleted.");
		}
		
		Node parentNode = hasRelationship.getStartNode();
		Relationship hadRelationship = parentNode.createRelationshipTo(node, RelationshipLabels.had);
		for(String key : hasRelationship.getPropertyKeys())
		{
			hadRelationship.setProperty(key, hasRelationship.getProperty(key));
		}
		hadRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
		
		hasRelationship.delete();
		return node;
	}
	
	public Node restoreNode(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		Node node = this.getNode(nodeId);
		Relationship hadRelationship = node.getSingleRelationship(RelationshipLabels.had, Direction.INCOMING);
		if(hadRelationship == null)
		{
			throw new NodeUnavailable("ERROR: Node is unavailable! - \"" + nodeId + "\". Could be it has not been deleted.");
		}
		
		Node parentNode = hadRelationship.getStartNode();
		Relationship hasRelationship = parentNode.createRelationshipTo(node, RelationshipLabels.has);
		for(String key : hadRelationship.getPropertyKeys())
		{
			hasRelationship.setProperty(key, hadRelationship.getProperty(key));
		}
		hadRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
		
		hadRelationship.delete();
		return node;
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
	
	public Node getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		Node user = this.getUser(userId);
		Iterable<Relationship> iterable = user.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		
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
	
	public Node getRootDirectory(String userId, String filesystemId, int filesystemVersion) throws UserNotFound, FilesystemNotFound, VersionNotFound
	{
		Node filesystem = this.getFilesystem(userId, filesystemId);
		Node versionedFilesystem = null;
		try
		{
			versionedFilesystem = this.getNodeVersion((String) filesystem.getProperty(MandatoryProperties.nodeId.name()), filesystemVersion);
		}
		catch (NodeNotFound | NodeUnavailable e) {}
		return versionedFilesystem.getSingleRelationship(RelationshipLabels.has, Direction.OUTGOING).getEndNode();
	}
	
	public Node getDirectory(String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound
	{
		Node rootDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		Iterable<Relationship> iterable = rootDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.Directory))
			{
				String retrievedDirectoryPath = (String) node.getProperty(MandatoryProperties.directoryPath.name());
				String retrievedDirectoryName = (String) node.getProperty(MandatoryProperties.directoryName.name());
				
				if(retrievedDirectoryPath.equals(directoryPath) && retrievedDirectoryName.equals(directoryName))
				{
					return node;
				}
			}
		}
		
		throw new DirectoryNotFound("ERROR: Directory not found! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
	}
	
	public List<Node> getAllDirectory(String userId, String filesystemId, int filesystemVersion) throws UserNotFound, FilesystemNotFound, VersionNotFound
	{
		List<Node> directoryList = new ArrayList<Node>();
		Node rootDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		Iterable<Relationship> iterable = rootDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.Directory))
			{
				directoryList.add(node);
			}
		}
		
		return directoryList;
	}
	
	public Node getFile(String userId, String filesystemId, int filesystemVersion, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound, FileNotFound
	{
		Node parentDirectory = null;
		if(filePath.equals("/"))
		{
			parentDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		}
		else
		{
			String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
			String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
			directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
			parentDirectory = this.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
		}
		
		Iterable<Relationship> iterable = parentDirectory.getRelationships(Direction.OUTGOING, RelationshipLabels.has);
		for(Relationship relationship : iterable)
		{
			Node node = relationship.getEndNode();
			if(node.hasLabel(NodeLabels.File))
			{
				String retrievedFilePath = (String) node.getProperty(MandatoryProperties.filePath.name());
				String retrievedFileName = (String) node.getProperty(MandatoryProperties.fileName.name());
				
				if(retrievedFilePath.equals(filePath) && retrievedFileName.equals(fileName))
				{
					return node;
				}
			}
		}
		
		throw new FileNotFound("ERROR: File not found! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
	}
	
	public Node copyNode(Node node)
	{
		String label = node.getLabels().iterator().next().name();
		NodeLabels nodeLabel = NodeLabels.valueOf(label);
		Node copyNode = this.createNode(nodeLabel);
		
		for(String key : node.getPropertyKeys())
		{
			if(! key.equals(MandatoryProperties.nodeId.name()))
			{
				copyNode.setProperty(key, node.getProperty(key));
			}
		}
		
		return copyNode;
	}
	
	public Node copyNodeTree(Node node, List<String> ignoreRelationships)
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
					if(ignoreRelationships.contains(relationship.getType().name()))
					{
						continue;
					}
					
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
