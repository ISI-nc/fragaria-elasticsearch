package nc.isi.fragaria_adapter_rewrite.dao;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;
import nc.isi.fragaria_adapter_rewrite.enums.Completion;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
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
		} else if (query instanceof ByViewQuery
				&& ((ByViewQuery<T>) query).getFilter().size() > 0) {
			BoolQueryBuilder esQuery = QueryBuilders.boolQuery();
			for (String prop : ((ByViewQuery<T>) query).getFilter().keySet()) {
				Object value = ((ByViewQuery<T>) query).getFilter().get(prop);
				MatchQueryBuilder propQuery = null;
				if (value != null) {
					if (value instanceof Entity) {
						propQuery = QueryBuilders.matchQuery(prop + "._id",
								((Entity) value).getId());
					} else if (value instanceof Enum) {
						propQuery = QueryBuilders.matchQuery(prop,
								((Enum) value).name());
					} else
						propQuery = QueryBuilders.matchQuery(prop, value);
				} else {
					propQuery = QueryBuilders.matchQuery(prop, value);
				}
				if (((ByViewQuery<T>) query).getFilter().keySet().size() > 1)
					propQuery.operator(Operator.AND);
				esQuery.must(propQuery);
			}
			return get(new SearchQuery<>(query.getResultType(), esQuery,
					DEFAULT_SIZE), false);
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
			BoolQueryBuilder esQuery = QueryBuilders.boolQuery();
			for (String prop : ((ByViewQuery<T>) query).getFilter().keySet()) {
				Object value = ((ByViewQuery<T>) query).getFilter().get(prop);
				MatchQueryBuilder propQuery = null;
				if (value != null) {
					if (value instanceof Entity) {
						propQuery = QueryBuilders.matchQuery(prop + "._id",
								((Entity) value).getId());
					} else if (value instanceof Enum) {
						propQuery = QueryBuilders.matchQuery(prop,
								((Enum) value).name());
					} else
						propQuery = QueryBuilders.matchQuery(prop, value);
				} else {
					propQuery = QueryBuilders.matchQuery(prop, value);
				}
				if (((ByViewQuery<T>) query).getFilter().keySet().size() > 1)
					propQuery.operator(Operator.AND);
				esQuery.must(propQuery);
			}
			return getUnique(new SearchQuery<>(query.getResultType(), esQuery,
					DEFAULT_SIZE), false);
		}
		return super.getUnique(query, cache);
	}

}
