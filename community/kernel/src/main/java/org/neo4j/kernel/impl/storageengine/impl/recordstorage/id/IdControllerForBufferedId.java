package org.neo4j.kernel.impl.storageengine.impl.recordstorage.id;

import java.util.function.Supplier;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;

public interface IdControllerForBufferedId extends IdController{

	void initialize( Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier );
}
