package nc.isi.fragaria_adapter_rewrite.dao;

import java.util.Collection;
import java.util.Iterator;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;
import nc.isi.fragaria_adapter_rewrite.entities.EntityMetadata;
import nc.isi.fragaria_adapter_rewrite.enums.Completion;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class ElastiSearchSessionImpl extends SessionImpl {

	private final static int DEFAULT_SIZE = 1000000;

	private final ElasticSearchAdapter elasticSearchAdapter;

	public ElastiSearchSessionImpl(AdapterManager adapterManager,
			EntityBuilder entityBuilder,
			ElasticSearchAdapter elasticSearchAdapter) {
		super(adapterManager, entityBuilder);
		this.elasticSearchAdapter = elasticSearchAdapter;
	}

	@Override
	public <T extends Entity> Collection<T> get(Query<T> query, boolean cache) {
		if (query instanceof SearchQuery) {
			Collection<T> entities = elasticSearchAdapter.executeQuery(
					(SearchQuery<T>) query).getResponse();
			// TODO We assume that objects from ES are full
			for (T entity : entities) {
				entity.setCompletion(Completion.FULL);
			}
			changeSession(entities);
			return entities;
		} else if (query instanceof ByViewQuery) {
			if (((ByViewQuery<T>) query).getFilter().size() > 0) {
				BoolQueryBuilder esQuery = convertByViewQueryToEsQuery(query);
				return get(new SearchQuery<>(query.getResultType(), esQuery,
						DEFAULT_SIZE), false);
			} else {
				return get(new SearchQuery<>(query.getResultType(),
						QueryBuilders.matchAllQuery(), DEFAULT_SIZE), false);
			}
		}
		return super.get(query, cache);
	}

	@Override
	public <T extends Entity> T getUnique(Query<T> query, boolean cache) {
		if (query instanceof SearchQuery) {
			Collection<T> collection = elasticSearchAdapter.executeQuery(
					(SearchQuery<T>) query).getResponse();
			// TODO We assume that objects from ES are full
			for (T entity : collection)
				entity.setCompletion(Completion.FULL);
			changeSession(collection);
			return collection.size() == 0 ? null : collection.iterator().next();
		} else if (query instanceof ByViewQuery
				&& ((ByViewQuery<T>) query).getFilter().size() > 0) {
			BoolQueryBuilder esQuery = convertByViewQueryToEsQuery(query);
			return getUnique(new SearchQuery<>(query.getResultType(), esQuery,
					DEFAULT_SIZE), false);
		}
		return super.getUnique(query, cache);
	}

	private <T extends Entity> BoolQueryBuilder convertByViewQueryToEsQuery(
			Query<T> query) {
		BoolQueryBuilder esQuery = QueryBuilders.boolQuery();
		for (String propName : ((ByViewQuery<T>) query).getFilter().keySet()) {
			Object value = ((ByViewQuery<T>) query).getFilter().get(propName);
			QueryBuilder propQuery = null;
			if (value != null) {
				EntityMetadata metadata = new EntityMetadata(
						query.getResultType());
				Class<?> propertyType = metadata.propertyType(propName);
				Class<?> collType = metadata.getCollectionType(propName);
				propName = metadata.getJsonPropertyName(propName);
				if (propertyType.isAssignableFrom(value.getClass())) {
					if (value instanceof Entity) {
						propQuery = QueryBuilders.matchQuery(propName + "._id",
								((Entity) value).getId());
					} else if (value instanceof Enum) {
						propQuery = QueryBuilders.matchQuery(propName,
								((Enum) value).name());
					} else if (value instanceof Class) {
						propQuery = QueryBuilders.matchQuery(propName,
								((Class) value).getCanonicalName());
					} else
						propQuery = QueryBuilders.matchQuery(propName, value);
				} else {
					if (value instanceof Iterable) {
						propQuery = QueryBuilders.boolQuery();
						for (Iterator colItem = ((Iterable) value).iterator(); colItem
								.hasNext();) {
							MatchQueryBuilder colItemQuery = null;
							Object valItem = colItem.next();
							if (collType == null) {
								if (Entity.class.isAssignableFrom(propertyType)) {
									colItemQuery = QueryBuilders.matchQuery(
											propName + "._id", valItem);
								} else if (valItem instanceof Enum) {
									colItemQuery = QueryBuilders.matchQuery(
											propName, ((Enum) valItem).name());
								} else if (valItem instanceof Class) {
									colItemQuery = QueryBuilders.matchQuery(
											propName, ((Class) valItem)
													.getCanonicalName());
								} else {
									colItemQuery = QueryBuilders.matchQuery(
											propName, valItem);
								}
								if (colItem.hasNext()) {
									colItemQuery.operator(Operator.AND);
								}
							} else {
								if (Entity.class.isAssignableFrom(collType)) {
									colItemQuery = QueryBuilders.matchQuery(
											propName + "._id", valItem);
								} else if (valItem instanceof Enum) {
									colItemQuery = QueryBuilders.matchQuery(
											propName, ((Enum) valItem).name());
								} else if (valItem instanceof Class) {
									colItemQuery = QueryBuilders.matchQuery(
											propName, ((Class) valItem)
													.getCanonicalName());
								} else {
									colItemQuery = QueryBuilders.matchQuery(
											propName, valItem);
								}
								colItemQuery.operator(Operator.AND);
							}
							((BoolQueryBuilder) propQuery).should(colItemQuery);
						}
					} else {
						if (Entity.class.isAssignableFrom(propertyType)) {
							propQuery = QueryBuilders.matchQuery(propName
									+ "._id", value);
						} else {
							propQuery = QueryBuilders.matchQuery(propName,
									value);
						}
					}

				}
				if (propQuery instanceof MatchQueryBuilder)
					((MatchQueryBuilder) propQuery).operator(Operator.AND);
			} else {
				propQuery = QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(),
						FilterBuilders.existsFilter(propName));
			}
			esQuery.must(propQuery);
		}
		return esQuery;
	}
}
