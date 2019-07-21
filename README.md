# Procedures for Benchmarks

Cypher Stored Procedures for Benchmarks

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.1/plugins/.
    

Restart your Neo4j Server. Your new Stored Procedures are available:

    CALL com.maxdemarzi.bi.q7(String tagName);
    CALL com.maxdemarzi.bi.q7("Arnold_Schwarzenegger");
    
    