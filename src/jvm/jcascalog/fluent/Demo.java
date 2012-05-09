package jcascalog.fluent;

import static jcascalog.fluent.JCascalog.query;
import static jcascalog.fluent.JCascalog.sdout;
import jcascalog.Playground;

public class Demo {

	public static void firstExemple(String[] args) {
		query("?person")
			.bind(Playground.AGE).to("?person", 25)
			.writeTo(sdout());
	}

	public static void secondExemple(String[] args) {
		query("?person", "?age", "?double-age")
			.bind(Playground.AGE).to("?person", 25)
			.mult("?age", 2).to("?double-age")
			.writeTo(sdout());
	}

}
