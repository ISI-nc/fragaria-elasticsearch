package nc.isi.fragaria_adapter_rewrite.services;

import nc.isi.fragaria_adapter_rewrite.dao.adapters.ElasticSearchAdapter;
import nc.isi.fragaria_adapter_rewrite.entities.elasticsearchaliases.AliasesGenerator;
import nc.isi.fragaria_adapter_rewrite.entities.elasticsearchaliases.AliasesGeneratorImpl;
import nc.isi.fragaria_adapter_rewrite.entities.elasticsearchaliases.AliasesInitializer;
import nc.isi.fragaria_ui.services.FragariaUIModule;

import org.apache.tapestry5.ioc.ServiceBinder;
import org.apache.tapestry5.ioc.annotations.Startup;
import org.apache.tapestry5.ioc.annotations.SubModule;

@SubModule({ FragariaDomainModule.class, FragariaUIModule.class })
public class FragariaElasticSearchModule {

	public static void bind(ServiceBinder binder) {
		binder.bind(ElasticSearchAdapter.class);
		binder.bind(AliasesInitializer.class);
		binder.bind(AliasesGenerator.class, AliasesGeneratorImpl.class);
	}

	@Startup
	public void initialize(AliasesInitializer aliasesInitializer) {
		// aliasesInitializer.initialize();
	}

}
