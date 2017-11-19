'use strict';

const Observable = require('rxjs').Observable;
const kafkaClient = require('../../../lib/client');
const consumerObservable = require('../../../lib/observables/consumer');

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

describe('consumer', () => {
    let subscription;

    describe('without autoCommit', () => {
        let options;

        beforeEach(()=> {
            options = Object.assign({ autoCommit: false }, opts);
        });

        it('should get a message as an observable', done => {
            const observable = consumerObservable.create('test_kafka', options);
            subscription = observable
                .subscribe(
                    ({ message, commit }) => {
                        const msg = message.value.toString('utf8');
                        expect(msg).toEqual('my message');
                        commit().subscribe(() => done());
                    },
                    err => done.fail(err)
                );

            delay(4000)
                .then(() => producer.publish('test_kafka', 'my message'));
        });

        it('should be able to map messages', done => {
            const observable = consumerObservable.create('test_kafka', options);
            subscription = observable
                .map( ({ message, commit }) => ({
                    commit,
                    message: message.value.toString('utf8'),
                    mapped: true
                }) )
                .subscribe(
                    ({ message, commit, mapped }) => {
                        expect(mapped).toBe(true);
                        expect(message).toEqual('my message');
                        commit().subscribe(() => done());
                    },
                    err => done.fail(err)
                );

            delay(5000)
                .then(() => producer.publish('test_kafka', 'my message'));
        });

        it('should be able to reduce messages', done => {
            const observable = consumerObservable.create('test_kafka', options);
            subscription = observable
                .do(m => m.commit().subscribe())
                .takeUntil(Observable.interval(6000))
                .reduce((acc, m) => acc + 1, 0)
                .subscribe(number => {
                    expect(number).toBe(3);
                    done();
                });

            delay(5000)
                .then(() => producer.publish('test_kafka', 'first message'))
                .then(() => producer.publish('test_kafka', 'second message'))
                .then(() => producer.publish('test_kafka', 'third message'));
        });

        it('should filter messages', done => {
            const observable = consumerObservable.create('test_kafka', options);
            subscription = observable
                /**
                 * here are have to concatMap through the commit because ".take"
                 * will immediately call unsubscribe and close the connection,
                 * so the last commit will fail if done after ".take"
                 */
                .concatMap(m => m.commit().map(() => m))
                .take(4)
                .map(({message}) => message.value.toString('utf8'))
                .filter(message => message.startsWith('a'))
                .reduce((acc, message) => acc + 1, 0)
                .subscribe(number => {
                    expect(number).toBe(2);
                    done();
                });

            delay(5000)
                .then(() => producer.publish('test_kafka', 'banana'))
                .then(() => producer.publish('test_kafka', 'avocado'))
                .then(() => delay(500))
                .then(() => producer.publish('test_kafka', 'coconut'))
                .then(() => producer.publish('test_kafka', 'apple'));
        });
    });

    describe('with autoCommit', () => {
        const getOffset = observable =>
            observable
                ._adapter
                .client()
                .offset('test_kafka');

        it('should get a message as an observable', done => {
            const observable = consumerObservable.create('test_kafka', opts);
            subscription = observable
                .subscribe(
                    ({ message }) => {
                        const msg = message.value.toString('utf8');
                        expect(msg).toEqual('my message');
                        done();
                    },
                    err => done.fail(err)
                );

            delay(5000)
                .then(() => producer.publish('test_kafka', 'my message'));
        });

        it('should commit the offset', done => {
            const observable = consumerObservable.create('test_kafka', opts);
            subscription = observable
                .take(1)
                .map(({ message }) => message.value.toString('utf8'))
                .subscribe(
                    msg => expect(msg).toEqual('my message'),
                    err => done.fail(err)
                );

            getOffset(observable)
                .then(startOffset => {
                    subscription.add(() => {
                        getOffset(observable)
                            .then(offset => expect(offset).toBe(startOffset + 1))
                            .then(done);
                    });
                });

            delay(5000)
                .then(() => producer.publish('test_kafka', 'my message'));
        });

        it('should filter messages', done => {
            const observable = consumerObservable.create('test_kafka', opts);
            subscription = observable
                .take(4)
                .map(({message}) => message.value.toString('utf8'))
                .filter(message => message.startsWith('a'))
                .reduce((acc, message) => acc + 1, 0)
                .subscribe(number => {
                    expect(number).toBe(2);
                    done();
                });

            delay(5000)
                .then(() => producer.publish('test_kafka', 'banana'))
                .then(() => producer.publish('test_kafka', 'avocado'))
                .then(() => producer.publish('test_kafka', 'coconut'))
                .then(() => producer.publish('test_kafka', 'apple'));
        });

        it('should not commit message if observer throws', done => {
            const observable = consumerObservable.create('test_kafka', opts);
            const adapter = observable._adapter;
            spyOn(adapter, 'commit').and.callThrough();

            subscription = observable
                .take(1)
                .map(({ message }) =>  message.value.toString('utf8'))
                .catch(err => done.fail(err))
                .subscribe({
                    next: () => { throw "Ops!" },
                    error: err => done.fail(err)
                });

            delay(5000)
                .then(() => producer.publish('test_kafka', 'my message'))
                .then(() => subscription.add(() => {
                    expect(adapter.commit).not.toHaveBeenCalled();
                    done();
                }));
        });

    });

    afterEach(() => {
        subscription.unsubscribe();
    });

});