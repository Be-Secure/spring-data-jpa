package org.springframework.data.jpa.repository.query;

import jakarta.persistence.Query;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.repository.core.support.SurroundingTransactionDetectorMethodInterceptor;

/**
 * Execute a JPA query for a Java 8 {@link java.util.stream.Stream}-returning repository method.
 */
class StreamExecutor extends JpaQueryContextExecutor {

    private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a streaming query method without a surrounding transaction that keeps the connection open so that the Stream can actually be consumed; Make sure the code consuming the stream uses @Transactional or any other way of declaring a (read-only) transaction";

    public StreamExecutor(AbstractJpaQueryContext queryContext) {
        super(queryContext);
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

        if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
            throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
        }

        return query.getResultStream();
    }
}
