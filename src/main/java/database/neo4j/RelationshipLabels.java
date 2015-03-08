package database.neo4j;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipLabels implements RelationshipType
{
	has, hasVersion
}
