package net.coacoas.rxhazelcast;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.nio.Address;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;

public class HazelcastObservableTest {
    private final Executor service = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "HazelcastObservableTest-" + count.incrementAndGet());
        }
    });
    private HazelcastInstance instance;

    @Before
    public void setup() {
        instance = newInstance(UUID.randomUUID().toString());
    }

    private Config getHazelcastConfig(String clusterName) {
        Config config = new Config();
        config.getNetworkConfig().getInterfaces().clear().addInterface("127.0.0.1");
        Join join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        config.getGroupConfig().setName(clusterName);
        return config;
    }

    public HazelcastInstance newInstance(String clusterName) {
        return Hazelcast.newHazelcastInstance(getHazelcastConfig(clusterName));
    }

    @After
    public void shutdownCluster() {
        instance.getLifecycleService().shutdown();
    }

    @Test
    public void testObservableCreationFromTopic() throws Exception {
        final ArrayList<String> responses = new ArrayList<>();
        String topicName = UUID.randomUUID().toString();
        ITopic<String> topic = instance.getTopic(topicName);
        Subscription s = HazelcastObservable.fromTopic(topic).
                map(new UpperCase()).
                subscribe(new AddToList(responses));
        System.out.println("Sending messages");


        topic.publish("Message");
        topic.publish("Correspondence");
        System.out.println("Done sending");
        Thread.sleep(30);

        Assert.assertThat(responses, is(Lists.newArrayList("MESSAGE", "CORRESPONDENCE")));
    }

    @Test
    public void testUnsubscribeFromTopic() throws Exception {
        final ArrayList<String> responses = new ArrayList<>();
        String topicName = UUID.randomUUID().toString();
        ITopic<String> topic = instance.getTopic(topicName);

        Subscription s = HazelcastObservable.fromTopic(topic).
                take(1).
                observeOn(Schedulers.from(service)).
                map(new UpperCase()).
                observeOn(Schedulers.from(service)).
                subscribe(new AddToList(responses));

        topic.publish("Message");
        for (int i = 0; i < 1000; i++) {
            topic.publish(Integer.toString(i));
        }


        Thread.sleep(30);

        Assert.assertThat(topic.getLocalTopicStats().getOperationStats().getNumberOfPublishes(), is(1001l));
        Assert.assertThat(topic.getLocalTopicStats().getOperationStats().getNumberOfReceivedMessages(), is(1l));
        Assert.assertThat(responses, is(Lists.newArrayList("MESSAGE")));
    }

    @Test
    public void testMultipleInstances() throws Exception {
        final String clusterName = UUID.randomUUID().toString();
        Config cfg = getHazelcastConfig(clusterName);
        HazelcastInstance first = Hazelcast.newHazelcastInstance(cfg);
        cfg.getNetworkConfig().
                getJoin().
                getTcpIpConfig().
                setEnabled(true);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().
                clear().addAddress(new Address(first.getCluster().getLocalMember().getInetSocketAddress()));
        HazelcastInstance second = Hazelcast.newHazelcastInstance(cfg);
        final ArrayList<String> responses = new ArrayList<>();

        String topicName = UUID.randomUUID().toString();
        ITopic<String> topic = first.getTopic(topicName);
        Subscription s = HazelcastObservable.fromTopic(topic).
                take(3).
                observeOn(Schedulers.from(service)).
                map(new UpperCase()).
                observeOn(Schedulers.from(service)).
                subscribe(new AddToList(responses));
        System.out.println("Sending messages");

        ITopic<String> secondTopic = second.getTopic(topicName);
        secondTopic.publish("Message");
        for (int i = 0; i < 1000; i++) {
            secondTopic.publish(Integer.toString(i));
        }
        System.out.println("Done sending");
        Thread.sleep(30);

        Assert.assertThat(responses, is(Lists.newArrayList("MESSAGE", "0", "1")));

        second.getLifecycleService().shutdown();
        first.getLifecycleService().shutdown();
    }

    private static class UpperCase implements Func1<String, String> {
        @Override
        public String call(String s) {
            System.out.println("To uppercase for " + s + " On thread " + Thread.currentThread().getName());
            return s.toUpperCase();
        }
    }

    private static class AddToList implements Action1<String> {
        private final ArrayList<String> responses;

        public AddToList(ArrayList<String> responses) {
            this.responses = responses;
        }

        @Override
        public void call(String s) {
            System.out.println("Adding " + s + " to array On thread " + Thread.currentThread().getName());
            responses.add(s);

        }
    }
}
