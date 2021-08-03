package cluster;

import java.time.Duration;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import cluster.DeviceEntityActor.Command;

class EntityQueryActor extends AbstractBehavior<DeviceEntityActor.Command> {
  private final ActorContext<DeviceEntityActor.Command> actorContext;
  private final ClusterSharding clusterSharding;
  private final int entitiesPerNode;
  private final Integer nodePort;
  private final TimerScheduler<DeviceEntityActor.Command> timerScheduler;

  static Behavior<DeviceEntityActor.Command> create() {
    return Behaviors.setup(actorContext -> 
        Behaviors.withTimers(timer -> new EntityQueryActor(actorContext, timer)));
  }

  private EntityQueryActor(ActorContext<Command> actorContext, TimerScheduler<DeviceEntityActor.Command> timerScheduler) {
    super(actorContext);
    this.actorContext = actorContext;
    clusterSharding = ClusterSharding.get(actorContext.getSystem());

    entitiesPerNode = actorContext.getSystem().settings().config().getInt("entity-actor.entities-per-node");
    nodePort = actorContext.getSystem().address().getPort().orElse(-1);
    this.timerScheduler = timerScheduler;
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(GetValueToEntity.class, t -> onTick(t))
        .onMessage(GetRandomValue.class, t -> ramdomValue())
        .onMessage(StartTick.class, t -> {
            final var interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.command-tick-interval-iso-8601"));
            timerScheduler.startTimerWithFixedDelay(new GetRandomValue(), interval);
          return this;
        })
        .onMessage(StopTick.class, t -> {
          timerScheduler.cancelAll();
          return this;
        })
        .onMessage(DeviceEntityActor.GetValueAck.class, this::onGetValueAck)
        .onMessage(DeviceEntityActor.GetValueAckNotFound.class, this::onGetValueAckNotFound)
        .build();
  }

  private Behavior<DeviceEntityActor.Command> onTick(GetValueToEntity tick) {
    final var id = new DeviceEntityActor.Id(tick.entityId);
    final var entityRef = clusterSharding.entityRefFor(DeviceEntityActor.entityTypeKey, tick.entityId);
    entityRef.tell(new DeviceEntityActor.GetValue(id, actorContext.getSelf()));
    return this;
  }

  private Behavior<DeviceEntityActor.Command> ramdomValue() {
    final var entityId = DeviceEntityActor.entityId(nodePort, (int) Math.round(Math.random() * entitiesPerNode));
    final var id = new DeviceEntityActor.Id(entityId);
    final var entityRef = clusterSharding.entityRefFor(DeviceEntityActor.entityTypeKey, entityId);
    entityRef.tell(new DeviceEntityActor.GetValue(id, actorContext.getSelf()));
    return this;
  }

  private Behavior<DeviceEntityActor.Command> onGetValueAck(DeviceEntityActor.GetValueAck getValueAck) {
    log().info("{}", getValueAck);
    return this;
  }

  private Behavior<DeviceEntityActor.Command> onGetValueAckNotFound(DeviceEntityActor.GetValueAckNotFound getValueAckNotFound) {
    log().info("{}", getValueAckNotFound);
    return this;
  }
    
  private Logger log() {
    return actorContext.getSystem().log();
  }

  public static class GetValueToEntity implements DeviceEntityActor.Command {
    private final String entityId;

    public GetValueToEntity(String entityId) {
      this.entityId = entityId;
    }
  }

  public static class GetRandomValue implements DeviceEntityActor.Command {
  
  }

  public static class StartTick implements DeviceEntityActor.Command {
  
  }
  public static class StopTick implements DeviceEntityActor.Command {
  
  }
}
