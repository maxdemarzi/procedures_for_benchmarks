package com.maxdemarzi;

import com.maxdemarzi.results.MapResult;
import com.maxdemarzi.results.ScoreResult;
import com.maxdemarzi.schema.Labels;
import com.maxdemarzi.schema.RelationshipTypes;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
import java.util.stream.Stream;

import static com.maxdemarzi.schema.Properties.ID;
import static com.maxdemarzi.schema.Properties.NAME;

public class BusinessIntelligence {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/neo4j.log`
    @Context
    public Log log;

    private static final Comparator<Pair<Long, Long>> pairFirstComparator = Comparator.comparing(Pair::first);
    private static final Comparator<Pair<Long, Long>> pairOtherComparator = Comparator.comparing(Pair::other);

    /*
MATCH (tag:Tag {name: $tag})
  MATCH (tag)<-[:HAS_TAG]-(message1:Message)-[:HAS_CREATOR]->(person1:Person)
  MATCH (tag)<-[:HAS_TAG]-(message2:Message)-[:HAS_CREATOR]->(person1)
  OPTIONAL MATCH (message2)<-[:LIKES]-(person2:Person)
  OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message3:Message)<-[like:LIKES]-(p3:Person)
  RETURN
    person1.id,
    count(DISTINCT like) AS authorityScore
  ORDER BY
    authorityScore DESC,
    person1.id ASC
  LIMIT 100

  Given a *Tag*, find all *Persons* (`person`) that ever created a *Message* (`message1`) with the given *Tag*.

  For each of these *Persons* (`person`) compute their "authority score" as follows\:
  * The "authority score" is the sum of "popularity scores" of the *Persons* (`person2`) that liked any of that *Person*'s *Messages* (`message2`)
    with the given *Tag*.
  * A *Person*'s (`person2`) "popularity score" is defined as the total number of likes on all of their *Messages* (`message3`).
 */
    @Procedure(name = "com.maxdemarzi.bi.q7", mode = Mode.READ)
    @Description("com.maxdemarzi.bi.q7(String tagName)")
    public Stream<ScoreResult> biq7(@Name(value = "tagName") String tagName) {
        Node tag = db.findNode(Labels.Tag, NAME, tagName);
        if (tag == null) {
            return Stream.empty();
        } else {
            HashMap<Long, Long> scores = new HashMap<>();
            ArrayList<Pair<Long, Long>> results = new ArrayList<>();
            Roaring64NavigableMap creators = new Roaring64NavigableMap();

            for (Relationship rel : tag.getRelationships(Direction.INCOMING, RelationshipTypes.HAS_TAG)){
                Node message = rel.getStartNode();
                for (Relationship rel2 : message.getRelationships(Direction.OUTGOING, RelationshipTypes.HAS_CREATOR)) {
                    if (!creators.contains(rel2.getEndNodeId())){
                        Node user = rel2.getEndNode();
                        creators.add(rel2.getEndNodeId());
                        long sum = 0L;
                        for (Relationship rel3 : message.getRelationships(Direction.INCOMING, RelationshipTypes.LIKES)) {
                            if (!scores.containsKey(rel3.getStartNodeId())) {
                                Node otherUser = rel3.getStartNode();
                                long score = 0L;
                                for (Relationship rel4 : otherUser.getRelationships(Direction.INCOMING, RelationshipTypes.HAS_CREATOR)) {
                                    Node message2 = rel4.getStartNode();
                                    score += message2.getDegree(RelationshipTypes.LIKES, Direction.INCOMING);
                                }
                                scores.put(rel3.getStartNodeId(), score);
                                sum += score;
                            } else {
                                sum += scores.get(rel3.getStartNodeId());
                            }
                        }
                        results.add(Pair.of(((Long)user.getProperty(ID)), sum));
                    }
                }
            }

            return results.stream()
                    .sorted(pairOtherComparator.reversed()
                            .thenComparing(pairFirstComparator))
                    .limit(100).map(ScoreResult::new);

        }

    }


}
