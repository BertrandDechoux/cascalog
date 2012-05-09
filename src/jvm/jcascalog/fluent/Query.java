package jcascalog.fluent;

import java.util.List;

import jcascalog.Predicate;
import cascading.tap.Tap;

public interface Query {
	
	void writeTo(Tap tap);	
	void writeTo(Tap... taps);
	void writeTo(String name, Tap tap);
	void writeTo(String name, Tap... taps);

	ToOptional equals(Object... fields);
	ToOptional gt(Object... fields);
	ToOptional gte(Object... fields);
	ToOptional lt(Object... fields);
	ToOptional lte(Object... fields);
	
	// TODO more bind with precise types
	// should allow tap and other query
	TosRequired bind(List<?> source);

	ToRequired avg(Object... fields);
	ToRequired div(Object... fields);
	ToRequired max(Object... fields);
	ToRequired min(Object... fields);
	ToRequired minus(Object... fields);
	ToRequired mult(Object... fields);
	ToRequired plus(Object... fields);
	ToRequired sum(Object... fields);
	
	ToRequired count();
	ToRequired distinctCout();
	
	OfRequired fixedSample(int amout);
	OfRequired limit(int amout);
	OfRequired limitRank(int amout);
	
    Query distinct(boolean shouldDistinct);
    Query sort(Object... fields);
    Query reverse(boolean shouldReverse);
    Query trap(Object trap);
    
    Query add(Predicate predicate);
	
}
