package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Execute a JPA query for a repository with an @{@link org.springframework.data.jpa.repository.Modifying} annotation
 * applied.
 */
class ModifyingExecutor extends JpaQueryContextExecutor {

    private final EntityManager entityManager;

    public ModifyingExecutor(AbstractJpaQueryContext queryContext, EntityManager entityManager) {

        super(queryContext);

        Assert.notNull(entityManager, "EntityManager must not be null");

        this.entityManager = entityManager;
    }

    @Override
    protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

        Class<?> returnType = queryContext.getQueryMethod().getReturnType();

        boolean isVoid = ClassUtils.isAssignable(returnType, Void.class);
        boolean isInt = ClassUtils.isAssignable(returnType, Integer.class);

        Assert.isTrue(isInt || isVoid,
                "Modifying queries can only use void or int/Integer as return type; Offending method: "
                        + queryContext.getQueryMethod());

        if (queryContext.getQueryMethod().getFlushAutomatically()) {
            entityManager.flush();
        }

        int result = query.executeUpdate();

        if (queryContext.getQueryMethod().getClearAutomatically()) {
            entityManager.clear();
        }

        return result;
    }
}
