package jcascalog.fluent;

import jcascalog.Api;
import cascading.tap.Tap;
import cascalog.StdoutTap;

public class JCascalog {
	
	public static QueryBuilder query(String... finalFields) {
		return new QueryBuilder(finalFields);
	}
	
	// TODO check return type of clojure functions
	// worst case : encapsulate
	public static Tap sdout() {
		return new StdoutTap();
	}

	public static Tap hfsTextline(String path) {
		return (Tap) Api.hfsTextline("hfs-textline");
	}

	public static Tap hfsSeqfile(String path) {
		return (Tap) Api.hfsSeqfile(path);
	}

}
