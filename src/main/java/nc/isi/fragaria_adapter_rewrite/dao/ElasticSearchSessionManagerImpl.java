package nc.isi.fragaria_adapter_rewrite.dao;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.AdapterManager;
import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.EntityBuilder;

public class ElasticSearchSessionManagerImpl extends SessionManagerImpl
		implements SessionManager {
	private final ElasticSearchAdapter elasticSearchAdapter;

	public ElasticSearchSessionManagerImpl(AdapterManager adapterManager,
			EntityBuilder entityBuilder,
			ElasticSearchAdapter elasticSearchAdapter) {
		super(adapterManager, entityBuilder);
		this.elasticSearchAdapter = elasticSearchAdapter;
	}

	@Override
	public Session create() {
		return new ElastiSearchSessionImpl(adapterManager, entityBuilder,
				elasticSearchAdapter);
	}

}
