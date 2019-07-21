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
