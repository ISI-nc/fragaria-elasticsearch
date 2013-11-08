package nc.isi.fragaria_ui.services;

import java.util.Collection;
import java.util.Map;

import nc.isi.fragaria_adapter_rewrite.dao.SearchQuery;
import nc.isi.fragaria_adapter_rewrite.entities.Entity;

public interface ElasticsearchConfig<T extends Entity> {
	public int getLimit();
	public int getOffset();
	public Class<T> getType();
	public SearchQuery<T> getSearchQuery(String partial);
	public Map<String,T> getResultsWithLabel(Collection<T> results);
}
