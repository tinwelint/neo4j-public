package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.txstate.TxStateHolder;

public interface TransactionalCursorDependencies extends TxStateHolder, AssertOpen
{
    AccessMode accessMode();
}
