package bookeeping.backend.database.neo4j;

import org.neo4j.graphdb.Label;

public enum NodeLabels implements Label
{
	AutoIncrement, User, Filesystem, Directory, File
}
