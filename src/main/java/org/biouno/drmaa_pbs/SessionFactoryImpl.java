package org.biouno.drmaa_pbs;

import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

public class SessionFactoryImpl extends SessionFactory {

	private final Session session = new SessionImpl();
	
	@Override
	public Session getSession() {
		return session;
	}
	
}
