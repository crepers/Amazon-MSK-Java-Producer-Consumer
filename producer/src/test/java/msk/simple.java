package msk;

import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class simple {

	@Test
	void testMessage() {
		Random rand = new Random();
		for (int i = 0; i < 5; i++) {
			System.out.println(String.format("%d|Hello MSK|%s", System.currentTimeMillis(), UUID.randomUUID()));
			try {
				Thread.sleep(rand.nextInt(100));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
