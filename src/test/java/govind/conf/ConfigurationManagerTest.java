package govind.conf;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigurationManagerTest {
	@Test
	public void getProperty() {
		assertEquals(ConfigurationManager.getProperty("gao"), "gaowenwen");
	}
}