package nc.isi.fragaria_adapter_rewrite.dao;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;
import nc.isi.fragaria_adapter_rewrite.enums.Completion;

public class ElastiSearchSessionImpl extends SessionImpl {

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
			// TODO We assume that object from ES are full
			for (T entity : entities) {
				entity.setCompletion(Completion.FULL);
			}
			return entities;
		}
		return super.get(query, cache);
	}

	@Override
	public <T extends Entity> T getUnique(Query<T> query, boolean cache) {
		if (query instanceof SearchQuery) {
			Collection<T> collection = elasticSearchAdapter.executeQuery(
					(SearchQuery<T>) query).getResponse();
			// TODO We assume that object from ES are full
			for (T entity : collection)
				entity.setCompletion(Completion.FULL);
			return collection.size() == 0 ? null : collection.iterator().next();
		}
		return super.getUnique(query, cache);
	}

}
