'use strict';

const Observable = require('rxjs').Observable;

const consumerObservable = require('../../lib/observables/consumer');
const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092'
};

describe('consumer', () => {
    let subscription, observable, producer;
    beforeEach(() => {
        observable = consumerObservable.create('test_kafka', opts);
        producer = mockProducer();
    });

    it('should get a message as an observable', done => {
        subscription = observable
            .subscribe(
                ({ message }) => {
                    const msg = message.value.toString('utf8');
                    expect(msg).toEqual('my message');
                    done();
                },
                err => done.fail(err)
            );

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    it('should automatically commit message', done => {
        const adapter = observable._adapter;
        spyOn(adapter, 'commit').and.callThrough();
        subscription = observable
            .take(1)
            .subscribe();

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'))
            .then(() => subscription.add(() => {
                expect(adapter.commit).toHaveBeenCalled();
                done();
            }));
    });

    it('should be able to map messages', done => {
        subscription = observable
            .map( ({ message }) => ({
                message: message.value.toString('utf8'),
                mapped: true
            }) )
            .subscribe(
                ({ message, mapped }) => {
                    expect(mapped).toBe(true);
                    expect(message).toEqual('my message');
                    done()
                },
                err => done.fail(err)
            );

        delay(100)
            .then(() => producer.publish('test_kafka', 'my message'));
    });

    it('should be able to reduce messages', done => {
        subscription = observable
            .takeUntil(Observable.interval(500))
            .reduce((acc, m) => acc + 1, 0)
            .subscribe(number => {
                expect(number).toBe(3);
                done();
            });

        delay(100)
            .then(() => producer.publish('test_kafka', 'first message'))
            .then(() => producer.publish('test_kafka', 'second message'))
            .then(() => producer.publish('test_kafka', 'third message'));
    });

    it('should filter messages', done => {
        subscription = observable
            .take(4)
            .map(({message}) => message.value.toString('utf8'))
            .filter(message => message.startsWith('a'))
            .reduce((acc, message) => acc + 1, 0)
            .subscribe(number => {
                expect(number).toBe(2);
                done();
            });

        delay(100)
            .then(() => producer.publish('test_kafka', 'abacate'))
            .then(() => producer.publish('test_kafka', 'banana'))
            .then(() => producer.publish('test_kafka', 'coconut'))
            .then(() => producer.publish('test_kafka', 'apple'));
    });

    it('should catch if an error is thrown before subscribe', done => {
        subscription = observable
            .map(({message}) => {
                const msg = message.value.toString('utf8');
                if (msg ==='throw') { throw 'Ops!' }
                return msg;
            })
            .catch(err => Observable.of('pass'))
            .subscribe(message => message !== 'throw' ? done() : done.fail());

        delay(100)
            .then(() => producer.publish('test_kafka', 'throw'));
    });

    it('should log error observer throw', done => {
        spyOn(bunyan, 'error');

        subscription = observable
            .subscribe(() => { throw 'Ops!'; });

        delay(100)
            .then(() => producer.publish('test_kafka', 'throw'))
            .then(() => subscription.add(()=> {
                expect(bunyan.error).toHaveBeenCalled();
                done();
            }));
    });

    it('observable has a non standard _adapter attached to it', () => {
        expect(observable._adapter).toBeDefined();
    });

    it('should use DefaultAssignmentStrategy if strategy is not found', done => {
        const options = Object.assign({strategy: 'ops'}, opts);
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe(() => {
                const client = observable._adapter.client();
                const strategy = client.strategies[0].strategy.constructor.name;
                expect(strategy).toEqual('DefaultAssignmentStrategy');
                done();
            });

        delay(100)
            .then(() => producer.publish('test_kafka', 'message'));
    });

    it('should error if strategy is invalid', done => {
        const options = Object.assign({strategy: 'ops'}, opts);
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe(() => {
                const client = observable._adapter.client();
                const strategy = client.strategies[0].strategy.constructor.name;
                expect(strategy).toEqual('DefaultAssignmentStrategy');
                done();
            });

        delay(100)
            .then(() => producer.publish('test_kafka', 'message'));
    });

    it('should error if groupId is not defined', done => {
        const options = Object.assign({}, opts, { groupId: undefined });
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should error if brokers is not defined', done => {
        const options = Object.assign({}, opts, { brokers: undefined });
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should error if brokers is not a string or an array', done => {
        const options = Object.assign({}, opts, { brokers: {} });
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should error if initial client subscription fails', done => {
        const options = Object.assign({metadata: 1}, opts);
        observable = consumerObservable.create('test_kafka', options);
        subscription = observable
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should work for null message', done => {
        observable = consumerObservable.create('test_kafka', opts);
        subscription = observable
            .subscribe(({message}) => {
                expect(message.value).toBeNull();
                done();
            });

        delay(100)
            .then(() => producer.publish('test_kafka', null));
    });

    describe('without auto-commit', () => {
        let options;

        beforeEach(() => {
            options = Object.assign({ autoCommit: false }, opts) ;
            observable = consumerObservable.create('test_kafka', options);
        });

        it('should have a commit function', done => {
            subscription = observable
                .subscribe(
                    ({ commit }) => {
                        expect(commit).toBeDefined();
                        expect(typeof commit).toEqual('function');
                        expect(commit() instanceof Observable).toBe(true);
                        done();
                    },
                    err => done.fail(err)
                );

            delay(100)
                .then(() => producer.publish('test_kafka', 'my message'));
        });

        it('should not commit message', done => {
            const adapter = observable._adapter;
            spyOn(adapter, 'commit').and.callThrough();
            subscription = observable
                .take(1)
                .subscribe();

            delay(100)
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