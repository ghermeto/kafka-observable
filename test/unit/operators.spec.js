'use strict';

const Observable = require('rxjs').Observable;
const kafkaClient = require('../../lib/client');
const KafkaObservable = require('../../index');

const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092'
};

const producer = kafkaClient.producer(opts);

describe('JSONMessage operator', () => {
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

        delay(100)
            .then(() => producer.publish('test_kafka', {key: 'test-value'}));
    });

    it('should error if not JSON', done => {
        const observable = KafkaObservable.fromTopic('test_kafka', opts);
        subscription = observable
            .take(1)
            .let(KafkaObservable.JSONMessage())
            .subscribe(() => done.fail(), err => done());

        delay(100)
            .then(() => producer.publish('test_kafka', 'test'));
    });

    it('should error if mapper throws', done => {
        const observable = KafkaObservable.fromTopic('test_kafka', opts);
        subscription = observable
            .take(1)
            .let(KafkaObservable.JSONMessage(x => { throw 'Ops!'; }))
            .subscribe(() => done.fail(), err => done());

        delay(100)
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

        delay(100)
            .then(() => producer.publish('test_kafka', {key: 'test-value'}));
    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});

describe('TextMessage operator', () => {
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

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    it('should error if mapper throws', done => {
        const observable = KafkaObservable.fromTopic('test_kafka', opts);
        subscription = observable
            .take(1)
            .let(KafkaObservable.TextMessage(x => { throw 'Ops!'; }))
            .subscribe(() => done.fail(), err => done());

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});