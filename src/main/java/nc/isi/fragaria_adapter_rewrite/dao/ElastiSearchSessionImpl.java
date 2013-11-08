package nc.isi.fragaria_adapter_rewrite.dao;

import java.util.Collection;

import nc.isi.fragaria_adapter_rewrite.dao.Query;
import nc.isi.fragaria_adapter_rewrite.dao.SessionImpl;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;

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
			return elasticSearchAdapter.executeQuery((SearchQuery<T>) query)
					.getResponse();
		}
		return super.get(query, cache);
	}

	@Override
	public <T extends Entity> T getUnique(Query<T> query, boolean cache) {
		if (query instanceof SearchQuery) {
			Collection<T> collection = elasticSearchAdapter.executeQuery(
					(SearchQuery<T>) query).getResponse();
			return collection.size() == 0 ? null : collection.iterator().next();
		}
		return super.getUnique(query, cache);
	}

}
