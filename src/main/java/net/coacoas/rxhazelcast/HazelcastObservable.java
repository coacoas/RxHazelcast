package net.coacoas.rxhazelcast;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

/**
 *
 */
public class HazelcastObservable {

    public static <T> Observable<T> fromTopic(final ITopic<T> topic) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> subscriber) {
                final MessageListener<T> listener = new ObservableMessageListener<>(subscriber);
                topic.addMessageListener(listener);
                subscriber.add(new Subscription() {
                    private boolean completed = false;

                    @Override
                    public void unsubscribe() {
                        completed = true;
                        topic.removeMessageListener(listener);
                        subscriber.onCompleted();
                    }

                    @Override
                    public boolean isUnsubscribed() {
                        return completed;
                    }
                });
            }
        });
    }

    private static class ObservableMessageListener<E> implements MessageListener<E> {
        private final Subscriber<? super E> subscriber;

        public ObservableMessageListener(Subscriber<? super E> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onMessage(Message<E> message) {
            subscriber.onNext(message.getMessageObject());
        }
    }
}
