package com.maxdemarzi.schema;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType {
    KNOWS,
    IS_LOCATED_IN,
    STUDY_AT,
    WORK_AT,
    HAS_TAG,
    LIKES,
    HAS_CREATOR

}

