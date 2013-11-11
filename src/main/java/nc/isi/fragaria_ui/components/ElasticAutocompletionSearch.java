package nc.isi.fragaria_ui.components;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import nc.isi.fragaria_adapter_rewrite.dao.Session;
import nc.isi.fragaria_adapter_rewrite.dao.SessionManager;
import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;
import nc.isi.fragaria_ui.services.ElasticsearchConfig;
import nc.isi.fragaria_ui.services.ElasticsearchConfigProvider;
import nc.isi.fragaria_ui.utils.events.autocompletionsearch.ObjectSelectedFromAutocompletion;

import org.apache.tapestry5.BindingConstants;
import org.apache.tapestry5.ComponentAction;
import org.apache.tapestry5.ValidationTracker;
import org.apache.tapestry5.annotations.BeginRender;
import org.apache.tapestry5.annotations.Environmental;
import org.apache.tapestry5.annotations.Parameter;
import org.apache.tapestry5.annotations.Persist;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.FormSupport;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;

public class ElasticAutocompletionSearch<T extends AbstractEntity> {

	@Parameter(value = "prop:componentResources.id", defaultPrefix = BindingConstants.LITERAL)
	private String clientId;

	@Parameter(value = "3")
	@Property
	private int minChars;

	@Parameter(required = true, defaultPrefix = BindingConstants.LITERAL, allowNull = false)
	private String key;

	@Property
	@Parameter
	private Session session;

	@Parameter
	private EventBus busRecorder;

	@Parameter(defaultPrefix = BindingConstants.LITERAL)
	@Property
	private String placeholder;

	@Parameter(defaultPrefix = BindingConstants.LITERAL)
	@Property
	private String txtFieldClass;

	@Parameter(value = "true", defaultPrefix = BindingConstants.LITERAL)
	@Property
	private Boolean btnSearch;

	@Persist
	@Property
	private String entry;

	@Persist
	private String prevEntry;

	@Persist
	private Map<String, T> map;

	@Persist
	private ElasticsearchConfig<T> autocompleteConfig;

	private String controlName;

	@Inject
	private SessionManager sessionManager;

	@Inject
	private ElasticsearchConfigProvider autocompleteConfigProvider;

	@Environmental
	private FormSupport formSupport;

	@Environmental
	private ValidationTracker tracker;

	private static final ProcessSubmission PROCESS_SUBMISSION = new ProcessSubmission();

	@SuppressWarnings("unchecked")
	@BeginRender
	public void initialize() {
		if (session == null)
			session = sessionManager.create();
		map = Maps.newHashMap();
		if (autocompleteConfig == null)
			autocompleteConfig = autocompleteConfigProvider
					.getAutocompleteConfig(key);
	}

	List<String> onProvideCompletionsFromEntry(String partial) {
		List<String> matches = new ArrayList<String>();
		if (prevEntry == null
				|| partial.length() == minChars
				|| partial.length() < prevEntry.length()
				|| (partial.length() > prevEntry.length() && map.values()
						.size() >= autocompleteConfig.getLimit())) {
			Collection<T> results = session.get(autocompleteConfig
					.getSearchQuery(partial));
			map.clear();
			map.putAll(autocompleteConfig.getResultsWithLabel(results));
		}
		matches.addAll(map.keySet());
		Collections.sort(matches);
		prevEntry = partial;
		return matches;
	}

	final void setupRender() {

		// If we are inside a form, ask FormSupport to execute "setup" now AND
		// store it in its list of actions to do on
		// submit.
		// If I contain other components, their actions will be stored later in
		// the list, after "setup". That is because
		// this method, setupRender(), is early in the sequence. This guarantees
		// "setup" will be executed on submit
		// BEFORE the components I contain are processed (including their
		// validation).

		if (formSupport != null) {
			String controlName = formSupport.allocateControlName(clientId);
			ComponentAction<ElasticAutocompletionSearch> setup = new Setup(
					controlName);
			formSupport.storeAndExecute(this, setup);
		}

	}

	private static class Setup implements
			ComponentAction<ElasticAutocompletionSearch> {
		private static final long serialVersionUID = 1L;

		private final String controlName;

		Setup(String controlName) {
			this.controlName = controlName;
		}

		public void execute(ElasticAutocompletionSearch component) {
			component.setup(controlName);
		}

		@Override
		public String toString() {
			return String
					.format(this.getClass().getSimpleName() + ".Setup[%s]",
							controlName);
		}
	}

	private void setup(String controlName) {
		this.setControlName(controlName);
	}

	final void afterRender() {

		// If we are inside a form, ask FormSupport to store PROCESS_SUBMISSION
		// in its list of actions to do on submit.
		// If I contain other components, their actions will already be in the
		// list, before PROCESS_SUBMISSION. That is
		// because this method, afterRender(), is late in the sequence. This
		// guarantees PROCESS_SUBMISSION will be
		// executed on submit AFTER the components I contain are processed
		// (which includes their validation).

		if (formSupport != null) {
			formSupport.store(this, PROCESS_SUBMISSION);
		}
	}

	private static class ProcessSubmission implements
			ComponentAction<ElasticAutocompletionSearch> {
		private static final long serialVersionUID = 1L;

		public void execute(ElasticAutocompletionSearch component) {
			component.processSubmission();
		}

		@Override
		public String toString() {
			return this.getClass().getSimpleName() + ".ProcessSubmission";
		}
	};

	private void processSubmission() {

		// Validate. We ensured in afterRender() that the components I contain
		// have already been validated.

		// Error if the number of persons chosen is less than specified by the
		// min parameter.

		if (entry == null || entry.length() < minChars) {
			tracker.recordError("Tapez " + minChars
					+ " caractères pour activer l'autocomplétion.");
			return;
		} else if (map.get(entry) == null) {
			tracker.recordError("\"" + entry + "\" n'existe pas.");
			return;
		} else {
			if (busRecorder != null) {
				busRecorder.post(new ObjectSelectedFromAutocompletion(map
						.get(entry)));
			}
		}

	}

	public String getControlName() {
		return controlName;
	}

	public void setControlName(String controlName) {
		this.controlName = controlName;
	}
}