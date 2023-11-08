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
import jakarta.persistence.Query;
import jakarta.persistence.QueryHint;
import jakarta.persistence.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A next generation base class representating a JPA query.
 *
 * @author Greg Turnquist
 */
public abstract class AbstractJpaQueryContext implements QueryContext {

	static final ConversionService CONVERSION_SERVICE;

	static {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
		conversionService.removeConvertible(Collection.class, Object.class);
		conversionService.removeConvertible(Object.class, Optional.class);

		CONVERSION_SERVICE = conversionService;
	}

	private final Optional<JpaQueryMethod> method;
	private final EntityManager entityManager;
	private final JpaMetamodel metamodel;
	private final PersistenceProvider provider;

	protected JpaQueryContextExecutor executor;

	protected final QueryParameterSetter.QueryMetadataCache metadataCache = new QueryParameterSetter.QueryMetadataCache();

	protected final Lazy<ParameterBinder> parameterBinder;

	public AbstractJpaQueryContext(Optional<JpaQueryMethod> method, EntityManager entityManager) {

		this.method = method;
		this.entityManager = entityManager;
		this.metamodel = JpaMetamodel.of(entityManager.getMetamodel());
		this.provider = PersistenceProvider.fromEntityManager(entityManager);
		this.parameterBinder = Lazy.of(this::createBinder);

		method.ifPresent(queryMethod -> {
			if (queryMethod.isStreamQuery()) {
				this.executor = new StreamExecutor(this);
			} else if (queryMethod.isProcedureQuery()) {
				this.executor = new ProcedureExecutor(this);
			} else if (queryMethod.isCollectionQuery()) {
				this.executor = new CollectionExecutor(this);
			} else if (queryMethod.isSliceQuery()) {
				this.executor = new SlicedExecutor(this);
			} else if (queryMethod.isPageQuery()) {
				this.executor = new PagedExecutor(this);
			} else if (queryMethod.isModifyingQuery()) {
				this.executor = new ModifyingExecutor(this, entityManager);
			} else {
				this.executor = new SingleEntityExecutor(this);
			}
		});

	}

	public void setExecutor(JpaQueryContextExecutor executor) {
		this.executor = executor;
	}

	@Nullable
	@Override
	public JpaQueryMethod getQueryMethod() {
		return this.method.orElse(null);
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

	final public Object execute() {
		return execute(new Object[0]);
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

		Class<?> typeToRead = getTypeToRead(method.get().getResultProcessor().getReturnedType());

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
		return ParameterBinderFactory.createBinder(method.get().getParameters());
	}

	/**
	 * Apply any {@link QueryHint}s to the {@link Query}.
	 * 
	 * @param query
	 * @return
	 */
	protected Query applyQueryHints(Query query) {

		method.ifPresent(queryMethod -> {

			List<QueryHint> hints = queryMethod.getHints();

			if (!hints.isEmpty()) {
				for (QueryHint hint : hints) {
					applyQueryHint(query, hint);
				}
			}

			// Apply any meta-attributes that exist
			if (queryMethod.hasQueryMetaAttributes()) {

				if (provider.getCommentHintKey() != null) {
					query.setHint( //
							provider.getCommentHintKey(),
							provider.getCommentHintValue(queryMethod.getQueryMetaAttributes().getComment()));
				}
			}
		});

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

		return method //
				.map(queryMethod -> {

					LockModeType lockModeType = queryMethod.getLockModeType();
					return lockModeType == null //
							? query //
							: query.setLockMode(lockModeType);
				}) //
				.orElse(query);
	}

	/**
	 * Bind the query arguments to the {@link Query}.
	 * 
	 * @param query
	 * @param accessor
	 * @return
	 */
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {
		return query;
	}

	/**
	 * Unwrap the results and apply any projections.
	 * 
	 * @param result
	 * @param accessor
	 * @return
	 */
	protected Object unwrapAndApplyProjections(@Nullable Object result, JpaParametersParameterAccessor accessor) {

		return method //
				.map(queryMethod -> {

					ResultProcessor withDynamicProjection = queryMethod.getResultProcessor().withDynamicProjection(accessor);

					// TODO: Migrate TupleConverter to its own class?
					return withDynamicProjection.processResult(result,
							new AbstractJpaQuery.TupleConverter(withDynamicProjection.getReturnedType()));
				}) //
				.orElse(result);
	}

	// Internals

	/**
	 * Transform the incoming array of arguments into a {@link JpaParametersParameterAccessor}.
	 * 
	 * @param values
	 * @return
	 */
	private JpaParametersParameterAccessor obtainParameterAccessor(Object[] values) {

		if (method.isPresent()) {

			if (method.get().isNativeQuery() && PersistenceProvider.HIBERNATE.equals(provider)) {
				return new HibernateJpaParametersParameterAccessor(method.get().getParameters(), values, entityManager);
			}

			return new JpaParametersParameterAccessor(method.get().getParameters(), values);
		} else {
			return new JpaParametersParameterAccessor(new JpaParameters(List.of()), values);
		}
	}

}
