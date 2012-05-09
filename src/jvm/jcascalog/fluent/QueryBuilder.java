package jcascalog.fluent;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Predicate;
import jcascalog.Subquery;
import jcascalog.op.Avg;
import jcascalog.op.Count;
import jcascalog.op.DistinctCount;
import jcascalog.op.Div;
import jcascalog.op.Equals;
import jcascalog.op.FixedSample;
import jcascalog.op.GT;
import jcascalog.op.GTE;
import jcascalog.op.LT;
import jcascalog.op.LTE;
import jcascalog.op.Limit;
import jcascalog.op.LimitRank;
import jcascalog.op.Max;
import jcascalog.op.Min;
import jcascalog.op.Minus;
import jcascalog.op.Multiply;
import jcascalog.op.Plus;
import jcascalog.op.Sum;
import cascading.tap.Tap;

// TODO we don't need jcascalog.op, subclass does not help
// TODO what is the trap option?
// TODO what is the difference between sum and +?
public class QueryBuilder implements Query, TosRequired, ToRequired, ToOptional, OfRequired {
	
	private final Object[] finalFields;
	private Object currentOp;
	private Object[] inFields;
	private List<Predicate> predicates = new LinkedList<Predicate>();
	
	public QueryBuilder(String... finalFields) {
		this.finalFields = finalFields;
	}
	
	@Override
	public Query to(Object field) {
		if(inFields == null) {
			predicates.add(new Predicate(currentOp, new Fields(field)));
		} else {
			predicates.add(new Predicate(currentOp, new Fields(inFields), new Fields(field)));
		}
		currentOp = null;
		inFields = null;
		return this;
	}
	
	@Override
	public Query to(Object... fields) {
		if(inFields == null) {
			predicates.add(new Predicate(currentOp, new Fields(fields)));
		} else {
			predicates.add(new Predicate(currentOp, new Fields(inFields), new Fields(fields)));
		}
		currentOp = null;
		inFields = null;
		return this;
	}
	
	@Override
	public Query of(Object fields) {
		predicates.add(new Predicate(currentOp, new Fields(fields)));
		currentOp = null;
		inFields = null;
		return this;
	}
	
	private ToOptional booleanPredicate(Object op,Object... fields) {
		keepPreviousPredicate();
		currentOp = op;
		inFields = fields;
		return this;
	}


	private ToRequired otherPredicate(Object op,Object... fields) {
		keepPreviousPredicate();
		currentOp = op;
		inFields = fields;
		return this;
	}

	private ToRequired headlessPredicate(Object op) {
		keepPreviousPredicate();
		currentOp = op;
		return this;
	}

	private OfRequired leglessPredicate(Object op) {
		keepPreviousPredicate();
		currentOp = op;
		return this;
	}
	
	private void keepPreviousPredicate() {
		if(currentOp != null) {
			of(inFields);
		}
	}

	public void writeTo(Tap tap) {
		Api.execute(defaultName(), Collections.singletonList(tap), generate());
	}
	
	public void writeTo(String name, Tap... taps) {
		Api.execute(name, Arrays.asList(taps), generate());
	}
	
	public void writeTo(String name, Tap tap) {
		Api.execute(name, Collections.singletonList(tap), generate());
	}
	
	public void writeTo(Tap... taps) {
		Api.execute(defaultName(), Arrays.asList(taps), generate());
	}
	
	private Subquery generate() {
		return new Subquery(new Fields(finalFields), predicates);
	}

	private String defaultName() {
		return "jcascalog-"+System.currentTimeMillis();
	}
	
	public ToOptional equals(Object... fields) {
		return booleanPredicate(new Equals(), fields);
	}
	
	public ToOptional gt(Object... fields) {
		return booleanPredicate(new GT(), fields);
	}
	
	public ToOptional gte(Object... fields) {
		return booleanPredicate(new GTE(), fields);
	}
	
	public ToOptional lt(Object... fields) {
		return booleanPredicate(new LT(), fields);
	}
	
	public ToOptional lte(Object... fields) {
		return booleanPredicate(new LTE(), fields);
	}
	
	public TosRequired bind(List<?> source) {
		keepPreviousPredicate();
		currentOp = source;
		return this;
	}

	public ToRequired avg(Object... fields) {
		return otherPredicate(new Avg(), fields);
	}
	
	public ToRequired div(Object... fields) {
		return otherPredicate(new Div(), fields);
	}
	
	public ToRequired max(Object... fields) {
		return otherPredicate(new Max(), fields);
	}
	
	public ToRequired min(Object... fields) {
		return otherPredicate(new Min(), fields);
	}
	
	public ToRequired minus(Object... fields) {
		return otherPredicate(new Minus(), fields);
	}
	
	public ToRequired mult(Object... fields) {
		return otherPredicate(new Multiply(), fields);
	}
	
	public ToRequired plus(Object... fields) {
		return otherPredicate(new Plus(), fields);
	}
	
	public ToRequired sum(Object... fields) {
		return otherPredicate(new Sum(), fields);
	}
	
	public ToRequired count() {
		return headlessPredicate(new Count());
	}
	
	public ToRequired distinctCout() {
		return headlessPredicate(new DistinctCount());
	}
	
	public OfRequired fixedSample(int amout) {
		return leglessPredicate(new FixedSample(amout));
	}
	
	public OfRequired limit(int amout) {
		return leglessPredicate(new Limit(amout));
	}
	
	public OfRequired limitRank(int amout) {
		return leglessPredicate(new LimitRank(amout));
	}

	@Override
	public Query distinct(boolean shouldDistinct) {
		keepPreviousPredicate();
		predicates.add(Option.distinct(shouldDistinct));
		return this;
	}

	@Override
	public Query sort(Object... fields) {
		keepPreviousPredicate();
		predicates.add(Option.sort(new Fields(fields)));
		return this;
	}

	@Override
	public Query reverse(boolean shouldReverse) {
		keepPreviousPredicate();
		predicates.add(Option.reverse(shouldReverse));
		return this;
	}

	@Override
	public Query trap(Object trap) {
		keepPreviousPredicate();
		predicates.add(Option.trap(trap));
		return this;
	}

	@Override
	public Query add(Predicate predicate) {
		keepPreviousPredicate();
		predicates.add(predicate);
		return this;
	}	
	
}
