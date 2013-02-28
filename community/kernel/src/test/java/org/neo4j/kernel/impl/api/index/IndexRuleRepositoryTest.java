package org.neo4j.kernel.impl.api.index;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class IndexRuleRepositoryTest
{

    @Test
    public void shouldIncludeIndexRuleAfterItsBeenAdded() throws Exception
    {
        // Given
        IndexRuleRepository repo = new IndexRuleRepository();

        // When
        repo.add( new IndexRule( 1, 10, 100l ) );

        // Then
        assertThat( asSet( asIterable( repo.getIndexedProperties( 10 ) )), equalTo(asSet( 100l )));
    }

}
