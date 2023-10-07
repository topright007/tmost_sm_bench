package com.sparkdan.tmost_state_machine_bench;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TmostStateMachineBenchApplication {

	public static void main(String[] args) throws InterruptedException, IOException {
		ConfigurableApplicationContext ctx = SpringApplication.run(TmostStateMachineBenchApplication.class, args);
		ctx.getBean(RunTests.class).runTests();
		ctx.close();
		System.exit(0);
	}
}
