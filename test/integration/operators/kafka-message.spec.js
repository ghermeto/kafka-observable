'use strict';

const Observable = require('rxjs').Observable;
const kafkaClient = require('../../../lib/client');
const KafkaObservable = require('../../../index');

/**
 * brokerRedirection is required because no-kafka client will try to use
 * container given hostname and not the ip specified in brokers
 */
const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092',
    brokerRedirection: { 'kafka:9092' : 'localhost:9092' }
};

const producer = kafkaClient.producer(opts);

describe('TextMessage', () => {
    let subscription;

    it('should get a message value as text', done => {
        const observable = KafkaObservable.fromTopic('test_kafka', opts);
        subscription = observable
            .take(1)
            .let(KafkaObservable.TextMessage())
            .subscribe(
                msg => expect(msg).toEqual('my message'),
                err => done.fail(err),
                () => done()
            );

        delay(4000)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});