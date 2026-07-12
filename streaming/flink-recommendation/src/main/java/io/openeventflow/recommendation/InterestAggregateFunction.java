package io.openeventflow.recommendation;

import io.openeventflow.recommendation.model.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.io.Serializable;
import java.util.*;

public final class InterestAggregateFunction implements AggregateFunction<Event, InterestAggregateFunction.Accumulator, InterestAggregateFunction.InterestProfile> {
  private final long halfLifeMs;
  public InterestAggregateFunction(long halfLifeMs) { if(halfLifeMs<=0) throw new IllegalArgumentException("halfLifeMs must be positive"); this.halfLifeMs=halfLifeMs; }
  public static final class Accumulator implements Serializable { final Map<String,Signal> categories=new HashMap<>(),brands=new HashMap<>(),priceBuckets=new HashMap<>(),contentTypes=new HashMap<>(),actions=new HashMap<>(); String userId; }
  private record Signal(double value,long at) implements Serializable {}
  public record InterestProfile(String userId, Map<String,Double> categories, Map<String,Double> brands, Map<String,Double> priceBuckets, Map<String,Double> contentTypes, Map<String,Double> actions, long asOf) implements Serializable {}
  @Override public Accumulator createAccumulator(){ return new Accumulator(); }
  @Override public Accumulator add(Event e, Accumulator a){ a.userId=e.userId(); add(a.categories,e.category(),e.timestamp()); add(a.brands,e.brand(),e.timestamp()); add(a.priceBuckets,e.priceBucket(),e.timestamp()); add(a.contentTypes,e.contentType(),e.timestamp()); add(a.actions,e.eventType(),e.timestamp()); return a; }
  private void add(Map<String,Signal> map,String key,long at){ if(key==null)return; Signal old=map.get(key); double value=1+(old==null?0:decay(old,at)); map.put(key,new Signal(value,at)); }
  private double decay(Signal signal,long at){ return signal.value*Math.pow(0.5,Math.max(0,at-signal.at)/(double)halfLifeMs); }
  private Map<String,Double> snapshot(Map<String,Signal> source,long at){ Map<String,Double> out=new TreeMap<>(); source.forEach((k,v)->out.put(k,decay(v,at))); return Collections.unmodifiableMap(out); }
  public InterestProfile snapshot(Accumulator a,long at){ return new InterestProfile(a.userId,snapshot(a.categories,at),snapshot(a.brands,at),snapshot(a.priceBuckets,at),snapshot(a.contentTypes,at),snapshot(a.actions,at),at); }
  @Override public InterestProfile getResult(Accumulator a){ long at=latest(a); return snapshot(a,at); }
  private long latest(Accumulator a){ return List.of(a.categories,a.brands,a.priceBuckets,a.contentTypes,a.actions).stream().flatMap(m->m.values().stream()).mapToLong(Signal::at).max().orElse(0); }
  @Override public Accumulator merge(Accumulator a,Accumulator b){ b.categories.forEach((k,v)->merge(a.categories,k,v)); b.brands.forEach((k,v)->merge(a.brands,k,v)); b.priceBuckets.forEach((k,v)->merge(a.priceBuckets,k,v)); b.contentTypes.forEach((k,v)->merge(a.contentTypes,k,v)); b.actions.forEach((k,v)->merge(a.actions,k,v)); if(a.userId==null)a.userId=b.userId; return a; }
  private void merge(Map<String,Signal> target,String key,Signal incoming){ Signal current=target.get(key); if(current==null){target.put(key,incoming);return;} long at=Math.max(current.at,incoming.at); target.put(key,new Signal(decay(current,at)+decay(incoming,at),at)); }
}
