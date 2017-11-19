'use strict';

const Observable = require('rxjs').Observable;
const consumerObservable = require('../../../lib/observables/consumer');
const producerObservable = require('../../../lib/observables/producer');

/**
 * brokerRedirection is required because no-kafka client will try to use
 * container given hostname and not the ip specified in brokers
 */
const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092',
    brokerRedirection: { 'kafka:9092' : 'localhost:9092' }
};

describe('producer', () => {
    let consumerSubscription, producerSubscription;

    it('should send a single message', done => {
        const msg = 'producer1';
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msg, opts);

        consumerSubscription = consumer
            .takeUntil(Observable.interval(6000))
            .subscribe(() => {}, err => done.fail(err), () => done());

        delay(5000)
            .then(() => {
                producerSubscription = producer
                    .subscribe(
                        ({topic, error}) => {
                            expect(topic).toEqual('test_kafka');
                            expect(error).toBeNull();
                        },
                        err => done.fail(err)
                    );
            });
    });

    it('should send many messages', done => {
        const msgs = ['first', 'second'];
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, opts);

        consumerSubscription = consumer
            .take(2)
            .subscribe(() => {}, err => done.fail(err), () => done());

        delay(5000)
            .then(() => {
                producerSubscription = producer
                    .subscribe(
                        ({topic, error}) => {
                            expect(topic).toEqual('test_kafka');
                            expect(error).toBeNull();
                        },
                        err => done.fail(err)
                    );
            });
    });

    it('should send messages from an observable', done => {
        const msgs = Observable.from(['first', 'second', 'third']);
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, opts);

        consumerSubscription = consumer
            .take(3)
            .subscribe(() => {}, err => done.fail(err), () => done());

        delay(5000)
            .then(() => {
                producerSubscription = producer
                    .subscribe(
                        ({topic, error}) => {
                            expect(topic).toEqual('test_kafka');
                            expect(error).toBeNull();
                        },
                        err => done.fail(err)
                    );
            });
    });

    afterEach(() => {
        consumerSubscription.unsubscribe();
        producerSubscription.unsubscribe();
    });

});
