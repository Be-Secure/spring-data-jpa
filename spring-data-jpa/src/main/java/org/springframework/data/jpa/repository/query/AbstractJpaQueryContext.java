/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import jakarta.persistence.QueryHint;
import jakarta.persistence.StoredProcedureQuery;
import jakarta.persistence.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.core.support.SurroundingTransactionDetectorMethodInterceptor;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * A next generation base class representating a JPA query.
 *
 * @author Greg Turnquist
 */
abstract class AbstractJpaQueryContext implements QueryContext {

	static final ConversionService CONVERSION_SERVICE;

	static {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
		conversionService.removeConvertible(Collection.class, Object.class);
		conversionService.removeConvertible(Object.class, Optional.class);

		CONVERSION_SERVICE = conversionService;
	}

	private final JpaQueryMethod method;
	private final EntityManager entityManager;
	private final JpaMetamodel metamodel;
	private final PersistenceProvider provider;

	protected JpaQueryContextExecutor executor;

	protected final QueryParameterSetter.QueryMetadataCache metadataCache = new QueryParameterSetter.QueryMetadataCache();

	protected final Lazy<ParameterBinder> parameterBinder;

	public AbstractJpaQueryContext(JpaQueryMethod method, EntityManager entityManager) {

		this.method = method;
		this.entityManager = entityManager;
		this.metamodel = JpaMetamodel.of(entityManager.getMetamodel());
		this.provider = PersistenceProvider.fromEntityManager(entityManager);
		this.parameterBinder = Lazy.of(this::createBinder);

		if (method.isStreamQuery()) {
			this.executor = new StreamExecutor(this);
		} else if (method.isProcedureQuery()) {
			this.executor = new ProcedureExecutor(this);
		} else if (method.isCollectionQuery()) {
			this.executor = new CollectionExecutor(this);
		} else if (method.isSliceQuery()) {
			this.executor = new SlicedExecutor(this);
		} else if (method.isPageQuery()) {
			this.executor = new PagedExecutor(this);
		} else if (method.isModifyingQuery()) {
			this.executor = new ModifyingExecutor(this, entityManager);
		} else {
			this.executor = new SingleEntityExecutor(this);
		}

	}

	@Override
	public JpaQueryMethod getQueryMethod() {
		return this.method;
	}

	public JpaMetamodel getMetamodel() {
		return metamodel;
	}

	public EntityManager getEntityManager() {
		return entityManager;
	}

	/**
	 * This is the fundamental flow that all JPA-based operations will take, whether it's a stored procedure, a custom
	 * finder, an {@literal @Query}-based method, or something else.
	 *
	 * @param parameters must not be {@literal null}.
	 * @return
	 */
	@Nullable
	@Override
	final public Object execute(Object[] parameters) {

		JpaParametersParameterAccessor accessor = obtainParameterAccessor(parameters);

		ContextualQuery initialQuery = createQuery(accessor);

		ContextualQuery processedQuery = postProcessQuery(initialQuery, accessor);

		Query jpaQuery = turnIntoJpaQuery(processedQuery, accessor);

		// Query jpaQueryWithSpecs = applySpecifications(jpaQuery, accessor);

		Query jpaQueryWithHints = applyQueryHints(jpaQuery);

		Query jpaQueryWithHintsAndLockMode = applyLockMode(jpaQueryWithHints);

		Query queryToExecute = bindParameters(jpaQueryWithHintsAndLockMode, accessor);

		// execute query
		Object rawResults = executor.executeQuery(queryToExecute, accessor);

		// Unwrap results
		Object unwrappedResults = unwrapAndApplyProjections(rawResults, accessor);

		// return results
		return unwrappedResults;
	}

	/**
	 * Every form of a JPA-based query must produce a string-based query.
	 *
	 * @return
	 */
	protected abstract ContextualQuery createQuery(JpaParametersParameterAccessor accessor);

	/**
	 * Taking the original query, apply any textual transformations needed to make the query runnable.
	 *
	 * @param query
	 * @return modified query
	 */
	protected ContextualQuery postProcessQuery(ContextualQuery query, JpaParametersParameterAccessor accessor) {
		return query;
	}

	/**
	 * Transform a string query into a JPA {@link Query}.
	 *
	 * @param query
	 * @return
	 */
	protected Query turnIntoJpaQuery(ContextualQuery query, JpaParametersParameterAccessor accessor) {

		Class<?> typeToRead = getTypeToRead(method.getResultProcessor().getReturnedType());

		return entityManager.createQuery(query.getQuery(), typeToRead);
	}

	/**
	 * Extract the property return type for this {@link JpaQueryMethod}.
	 * 
	 * @param returnedType
	 * @return {@link Class} representation
	 */
	@Nullable
	protected Class<?> getTypeToRead(ReturnedType returnedType) {

		if (PersistenceProvider.ECLIPSELINK.equals(provider)) {
			return null;
		}

		return returnedType.isProjecting() && !getMetamodel().isJpaManaged(returnedType.getReturnedType()) //
				? Tuple.class //
				: null;
	}

	/**
	 * For a {@link Pageable}-based query, generate the corresponding count {@link Query}.
	 * 
	 * @param values
	 * @return
	 */
	protected Query createCountQuery(JpaParametersParameterAccessor values) {
		throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support count queries");
	}

	/**
	 * Create the {@link ParameterBinder} needed to associate arguments with the query.
	 * 
	 * @return
	 */
	protected ParameterBinder createBinder() {
		return ParameterBinderFactory.createBinder(method.getParameters());
	}

	/**
	 * Apply any {@link QueryHint}s to the {@link Query}.
	 * 
	 * @param query
	 * @return
	 */
	protected Query applyQueryHints(Query query) {

		List<QueryHint> hints = method.getHints();

		if (!hints.isEmpty()) {
			for (QueryHint hint : hints) {
				applyQueryHint(query, hint);
			}
		}

		// Apply any meta-attributes that exist
		if (method.hasQueryMetaAttributes()) {

			if (provider.getCommentHintKey() != null) {
				query.setHint( //
						provider.getCommentHintKey(), provider.getCommentHintValue(method.getQueryMetaAttributes().getComment()));
			}
		}

		return query;
	}

	/**
	 * Apply a {@link QueryHint} to the {@link Query}.
	 * 
	 * @param query
	 * @param hint
	 */
	protected void applyQueryHint(Query query, QueryHint hint) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(hint, "QueryHint must not be null");

		query.setHint(hint.name(), hint.value());
	}

	/**
	 * Apply the {@link LockModeType} to the {@link Query}.
	 * 
	 * @param query
	 * @return
	 */
	protected Query applyLockMode(Query query) {

		LockModeType lockModeType = method.getLockModeType();
		return lockModeType == null ? query : query.setLockMode(lockModeType);
	}

	/**
	 * Bind the query arguments to the {@link Query}.
	 * 
	 * @param query
	 * @param accessor
	 * @return
	 */
	protected abstract Query bindParameters(Query query, JpaParametersParameterAccessor accessor);

	/**
	 * Unwrap the results and apply any projections.
	 * 
	 * @param result
	 * @param accessor
	 * @return
	 */
	protected Object unwrapAndApplyProjections(@Nullable Object result, JpaParametersParameterAccessor accessor) {

		ResultProcessor withDynamicProjection = method.getResultProcessor().withDynamicProjection(accessor);

		// TODO: Migrate TupleConverter to its own class?
		return withDynamicProjection.processResult(result,
				new AbstractJpaQuery.TupleConverter(withDynamicProjection.getReturnedType()));
	}

	// Internals

	/**
	 * Transform the incoming array of arguments into a {@link JpaParametersParameterAccessor}.
	 * 
	 * @param values
	 * @return
	 */
	private JpaParametersParameterAccessor obtainParameterAccessor(Object[] values) {

		if (method.isNativeQuery() && PersistenceProvider.HIBERNATE.equals(provider)) {
			return new HibernateJpaParametersParameterAccessor(method.getParameters(), values, entityManager);
		}

		return new JpaParametersParameterAccessor(method.getParameters(), values);
	}

	/**
	 * Base class that defines how queries are executed with JPA.
	 * 
	 * @author Greg Turnquist
	 */
	abstract class JpaQueryContextExecutor {

		static final ConversionService CONVERSION_SERVICE;

		static {

			ConfigurableConversionService conversionService = new DefaultConversionService();

			conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
			conversionService.removeConvertible(Collection.class, Object.class);
			conversionService.removeConvertible(Object.class, Optional.class);

			CONVERSION_SERVICE = conversionService;
		}

		protected final AbstractJpaQueryContext queryContext;

		public JpaQueryContextExecutor(AbstractJpaQueryContext queryContext) {
			this.queryContext = queryContext;
		}

		/**
		 * Core function that defines the flow of executing a {@link Query}.
		 * 
		 * @param query
		 * @param accessor
		 * @return
		 */
		@Nullable
		public Object executeQuery(Query query, JpaParametersParameterAccessor accessor) {

			Assert.notNull(queryContext, "QueryContext must not be null");
			Assert.notNull(query, "Query must not be null");
			Assert.notNull(accessor, "JpaParametersParameterAccessor must not be null");

			Object result;

			try {
				result = doExecute(query, accessor);
			} catch (NoResultException ex) {
				return null;
			}

			if (result == null) {
				return null;
			}

			Class<?> requiredType = queryContext.getQueryMethod().getReturnType();

			if (ClassUtils.isAssignable(requiredType, void.class) || ClassUtils.isAssignableValue(requiredType, result)) {
				return result;
			}

			return CONVERSION_SERVICE.canConvert(result.getClass(), requiredType) //
					? CONVERSION_SERVICE.convert(result, requiredType) //
					: result;
		}

		/**
		 * Execute the query itself.
		 * 
		 * @param query
		 * @param accessor
		 * @return
		 */
		@Nullable
		protected abstract Object doExecute(Query query, JpaParametersParameterAccessor accessor);
	}

	/**
	 * Execute a JPA query for a Java 8 {@link java.util.stream.Stream}-returning repository method.
	 */
	private class StreamExecutor extends JpaQueryContextExecutor {

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

	/**
	 * Execute a JPA stored procedure.
	 */
	private class ProcedureExecutor extends JpaQueryContextExecutor {

		private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a @Procedure method without a surrounding transaction that keeps the connection open so that the ResultSet can actually be consumed; Make sure the consumer code uses @Transactional or any other way of declaring a (read-only) transaction";
		private final StoredProcedureQueryContext storedProcedureContext;

		public ProcedureExecutor(AbstractJpaQueryContext context) {

			super(context);

			Assert.isInstanceOf(StoredProcedureQueryContext.class, context);
			this.storedProcedureContext = (StoredProcedureQueryContext) context;
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

			Assert.isInstanceOf(StoredProcedureQuery.class, query);
			StoredProcedureQuery procedure = (StoredProcedureQuery) query;

			try {
				boolean returnsResultSet = procedure.execute();

				if (returnsResultSet) {

					if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
						throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
					}

					return storedProcedureContext.getQueryMethod().isCollectionQuery() ? procedure.getResultList()
							: procedure.getSingleResult();
				}

				return storedProcedureContext.extractOutputValue(procedure); // extract output value from the procedure
			} finally {
				if (procedure instanceof AutoCloseable autoCloseable) {
					try {
						autoCloseable.close();
					} catch (Exception ignored) {}
				}
			}
		}
	}

	/**
	 * Execute a JPA query for a Java {@link Collection}-returning repository method.
	 */
	private class CollectionExecutor extends JpaQueryContextExecutor {

		public CollectionExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
			return query.getResultList();
		}
	}

	/**
	 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Slice}-returning repository method.
	 */
	private class SlicedExecutor extends JpaQueryContextExecutor {

		public SlicedExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

			Pageable pageable = accessor.getPageable();

			int pageSize = 0;
			if (pageable.isPaged()) {

				pageSize = pageable.getPageSize();
				query.setMaxResults(pageSize + 1);
			}

			List<?> resultList = query.getResultList();

			boolean hasNext = pageable.isPaged() && resultList.size() > pageSize;

			if (hasNext) {
				return new SliceImpl<>(resultList.subList(0, pageSize), pageable, hasNext);
			} else {
				return new SliceImpl<>(resultList, pageable, hasNext);
			}
		}
	}

	/**
	 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Page}-returning repository method.
	 */
	private class PagedExecutor extends JpaQueryContextExecutor {

		public PagedExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
			return PageableExecutionUtils.getPage(query.getResultList(), accessor.getPageable(),
					() -> count(queryContext, accessor));
		}

		private long count(AbstractJpaQueryContext queryContext, JpaParametersParameterAccessor accessor) {

			Query countQuery = queryContext.createCountQuery(accessor);

			List<?> totals = countQuery.getResultList();

			return totals.size() == 1 //
					? CONVERSION_SERVICE.convert(totals.get(0), Long.class) //
					: totals.size();
		}

	}

	/**
	 * Execute a JPA query for a repository with an @{@link org.springframework.data.jpa.repository.Modifying} annotation
	 * applied.
	 */
	private class ModifyingExecutor extends JpaQueryContextExecutor {

		private final EntityManager entityManager;

		public ModifyingExecutor(AbstractJpaQueryContext queryContext, EntityManager entityManager) {

			super(queryContext);

			Assert.notNull(entityManager, "EntityManager must not be null");

			this.entityManager = entityManager;
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

			Class<?> returnType = method.getReturnType();

			boolean isVoid = ClassUtils.isAssignable(returnType, Void.class);
			boolean isInt = ClassUtils.isAssignable(returnType, Integer.class);

			Assert.isTrue(isInt || isVoid,
					"Modifying queries can only use void or int/Integer as return type; Offending method: " + method);

			if (method.getFlushAutomatically()) {
				entityManager.flush();
			}

			int result = query.executeUpdate();

			if (method.getClearAutomatically()) {
				entityManager.clear();
			}

			return result;
		}
	}

	/**
	 * Execute a JPA query for a repository method returning a single value.
	 */
	private class SingleEntityExecutor extends JpaQueryContextExecutor {

		public SingleEntityExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
			return query.getSingleResult();
		}
	}

	/**
	 * Execute a JPA query for a repository method using the Scroll API.
	 */
	class ScrollExecutor extends JpaQueryContextExecutor {
		private final Sort sort;
		private final ScrollDelegate<?> delegate;

		public ScrollExecutor(AbstractJpaQueryContext queryContext, Sort sort, ScrollDelegate<?> delegate) {

			super(queryContext);

			this.sort = sort;
			this.delegate = delegate;
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

			ScrollPosition scrollPosition = accessor.getScrollPosition();

			return delegate.scroll(query, sort.and(accessor.getSort()), scrollPosition);
		}
	}

	/**
	 * Execute a JPA delete.
	 */
	class DeleteExecutor extends JpaQueryContextExecutor {
		public DeleteExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {

			List<?> resultList = query.getResultList();

			for (Object o : resultList) {
				getEntityManager().remove(o);
			}

			return getQueryMethod().isCollectionQuery() ? resultList : resultList.size();
		}
	}

	/**
	 * Execute a JPA exists operation.
	 */
	class ExistsExecutor extends JpaQueryContextExecutor {

		public ExistsExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(Query query, JpaParametersParameterAccessor accessor) {
			return !query.getResultList().isEmpty();
		}
	}
}
