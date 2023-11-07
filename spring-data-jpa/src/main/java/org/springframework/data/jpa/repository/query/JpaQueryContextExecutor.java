package org.springframework.data.jpa.repository.query;

import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;

import java.util.Collection;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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

		if (queryContext.getQueryMethod() != null) {

			Class<?> requiredType = queryContext.getQueryMethod().getReturnType();

			if (ClassUtils.isAssignable(requiredType, void.class) || ClassUtils.isAssignableValue(requiredType, result)) {
				return result;
			}

			return CONVERSION_SERVICE.canConvert(result.getClass(), requiredType) //
					? CONVERSION_SERVICE.convert(result, requiredType) //
					: result;
		} else {
			return result;
		}
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
