package nc.isi.fragaria_ui.utils.events.autocompletionsearch;

import nc.isi.fragaria_adapter_rewrite.entities.AbstractEntity;
import nc.isi.fragaria_ui.utils.events.AbstractObjectEvent;

public class ObjectSelectedFromAutocompletion extends AbstractObjectEvent<AbstractEntity>{

	public ObjectSelectedFromAutocompletion(AbstractEntity object) {
		super(object);
	}
	
}
