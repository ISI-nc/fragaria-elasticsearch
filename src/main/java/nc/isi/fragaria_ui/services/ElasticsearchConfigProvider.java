package nc.isi.fragaria_ui.services;



import java.util.Map;


public class ElasticsearchConfigProvider {
	Map<String,ElasticsearchConfig> autocompleteConfigurations;
	
	public ElasticsearchConfigProvider(Map<String, ElasticsearchConfig> autocompleteConfigurations) {
		this.autocompleteConfigurations = autocompleteConfigurations;
	}
	
	public ElasticsearchConfig getAutocompleteConfig(String key){
		return autocompleteConfigurations.get(key);
	}
}
