'use strict';

const Observable = require('rxjs').Observable;
const KafkaObservable = require('../../index');

const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092'
};

describe('fromTopic', () => {
    let subscription;
    beforeEach(() => {

    });

    it('should inherit options', done => {
        const producer = mockProducer();
        const consumer = KafkaObservable(opts).fromTopic('test_kafka');

        subscription = consumer
            .take(1)
            .map(({message}) => message.value.toString('utf8'))
            .subscribe(
                message => expect(message).toEqual('my message'),
                err => done.fail(err),
                () => done()
            );

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    it('should accept options', done => {
        const producer = mockProducer();
        const consumer = KafkaObservable.fromTopic('test_kafka', opts);

        subscription = consumer
            .take(1)
            .map(({message}) => message.value.toString('utf8'))
            .subscribe(
                message => expect(message).toEqual('my message'),
                err => done.fail(err),
                () => done()
            );

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});

describe('toTopic', () => {
    let consumerSubscription, producerSubscription;

    it('should send a message', done => {
        const msg = 'message';
        const observable = KafkaObservable(opts);
        const consumer = observable.fromTopic('test_kafka');
        const producer = observable.toTopic('test_kafka', msg);

        consumerSubscription = consumer
            .take(1)
            .subscribe(() => {}, err => done.fail(err), () => done());

        delay(100)
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