package cluster;

import java.time.Duration;
import java.util.Date;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.OnMemberStatusChangedListener;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import cluster.EntityActor.Command;

class EntityCommandActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<EntityActor.Command> actorContext;
  private final ClusterSharding clusterSharding;
  private final int entitiesPerNode;
  private final Integer nodePort;
  private final TimerScheduler<EntityActor.Command> timerScheduler;

  static Behavior<EntityActor.Command> create() {
    return Behaviors.setup(actorContext -> 
        Behaviors.withTimers(timer -> new EntityCommandActor(actorContext, timer)));
  }

  private EntityCommandActor(ActorContext<Command> actorContext, TimerScheduler<EntityActor.Command> timerScheduler) {
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
        .onMessage(SendValueToEntity.class, t -> sendValueToEntity(t))
        .onMessage(SendRandomValue.class, t -> ramdomValue())
        .onMessage(StartTick.class, t -> {
            final var interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.command-tick-interval-iso-8601"));
            timerScheduler.startTimerWithFixedDelay(new SendRandomValue(), interval);
          return this;
        })
        .onMessage(StopTick.class, t -> {
          timerScheduler.cancelAll();
          return this;
        })
        .onMessage(EntityActor.ChangeValueAck.class, this::onChangeValueAck)
        .build();
  }

  private Behavior<EntityActor.Command> sendValueToEntity(SendValueToEntity tick) {
    final var id = new EntityActor.Id(tick.entityId);
    final var value = new EntityActor.Value(tick.value);
    final var entityRef = clusterSharding.entityRefFor(EntityActor.entityTypeKey, tick.entityId);
    entityRef.tell(new EntityActor.ChangeValue(id, value, actorContext.getSelf()));
    return this;
  }

  private Behavior<EntityActor.Command> ramdomValue() {
    final var entityId = EntityActor.entityId(nodePort, (int) Math.round(Math.random() * entitiesPerNode));
    final var id = new EntityActor.Id(entityId);
    final var value = new EntityActor.Value(new Date());
    final var entityRef = clusterSharding.entityRefFor(EntityActor.entityTypeKey, entityId);
    entityRef.tell(new EntityActor.ChangeValue(id, value, actorContext.getSelf()));
    return this;
  }

  private Behavior<EntityActor.Command> onChangeValueAck(EntityActor.ChangeValueAck changeValueAck) {
    log().info("{}", changeValueAck);
    return this;
  }

  private Logger log() {
    return actorContext.getSystem().log();
  }

  public static class SendValueToEntity implements EntityActor.Command {
    final String entityId;
    final String value;

    public SendValueToEntity(String entityId, String value) {
      this.entityId = entityId;
      this.value = value;
    }
  }

  public static class SendRandomValue implements EntityActor.Command {
  
  }

  public static class StartTick implements EntityActor.Command {
  
  }
  public static class StopTick implements EntityActor.Command {
  
  }
}
