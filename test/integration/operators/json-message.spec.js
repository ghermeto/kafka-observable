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

describe('JSONMessage', () => {
    let subscription;

    it('should get a message value as JSON', done => {
        const observable = KafkaObservable.fromTopic('test_kafka', opts);
        subscription = observable
            .take(1)
            .let(KafkaObservable.JSONMessage())
            .subscribe(
                json => {
                    expect(json.key).toBeDefined();
                    expect(json.key).toEqual('test-value');
                },
                err => done.fail(err),
                () => done()
            );

        delay(5000)
            .then(() => producer.publish('test_kafka', {key: 'test-value'}));
    });

    it('should be available when called in the instance', done => {
        const kafka = KafkaObservable(opts);
        const observable = kafka.fromTopic('test_kafka');
        subscription = observable
            .take(1)
            .let(kafka.JSONMessage())
            .subscribe(
                json => {
                    expect(json.key).toBeDefined();
                    expect(json.key).toEqual('test-value');
                },
                err => done.fail(err),
                () => done()
            );

        delay(5000)
            .then(() => producer.publish('test_kafka', {key: 'test-value'}));
    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});