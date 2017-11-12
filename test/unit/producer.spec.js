'use strict';

const Observable = require('rxjs').Observable;
const consumerObservable = require('../../lib/observables/consumer');
const producerObservable = require('../../lib/observables/producer');

const opts = {
    groupId: 'test',
    brokers: '127.0.0.1:9092'
};

describe('producer', () => {
    let consumerSubscription, producerSubscription;

    it('should send a single message', done => {
        const msg = 'message';
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msg, opts);

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

    it('should send many messages', done => {
        const msgs = ['first', 'second'];
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, opts);

        consumerSubscription = consumer
            .take(2)
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

    it('should send messages from an observable', done => {
        const msgs = Observable.from(['first', 'second', 'third']);
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, opts);

        consumerSubscription = consumer
            .take(3)
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

    it('should error if brokers is not defined', done => {
        const msgs = ['first', 'second'];
        const options = Object.assign({}, opts, { brokers: undefined });
        const producer = producerObservable.create('test_kafka', msgs, options);
        producerSubscription = producer
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should error if brokers is not a string or an array', done => {
        const msgs = ['first', 'second'];
        const options = Object.assign({}, opts, { brokers: {} });
        const producer = producerObservable.create('test_kafka', msgs, options);
        producerSubscription = producer
            .subscribe({ next: () => done.fail(), error: err => done() });
    });

    it('should propagate error if observable gives an error', done => {
        const msgs = Observable.throw('test');
        const producer = producerObservable.create('test_kafka', msgs, opts);
        producerSubscription = producer
            .subscribe({
                next: () => done.fail(),
                error: err => done(),
                complete: () => done()
            });
    });

    it('should default to DefaultPartitioner', done => {
        const msgs = ['first', 'second'];
        const options = Object.assign({ partitioner: 'NotFound'}, opts);
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, options);
        consumerSubscription = consumer
            .take(2)
            .subscribe(() => {}, err => done.fail(err), () => done());

        delay(100)
            .then(() => {
                producerSubscription = producer
                    .subscribe(() => {
                            const client = producer._adapter.client();
                            const partitioner = client.options.partitioner;

                            expect(partitioner.constructor.name)
                                .toEqual('DefaultPartitioner');

                            done();
                        },
                        err => done.fail(err)
                    );
            });

    });

    it('should serialize an object as message', done => {
        const msgs = [{ object: true }];
        const consumer = consumerObservable.create('test_kafka', opts);
        const producer = producerObservable.create('test_kafka', msgs, opts);
        consumerSubscription = consumer
            .take(1)
            .map(({message}) => message.value.toString('utf8'))
            .map(message => JSON.parse(message))
            .subscribe(
                json => expect(json.object).toBe(true),
                err => done.fail(err),
                () => done()
            );

        delay(100)
            .then(() => {
                producerSubscription = producer
                    .subscribe(() => {}, err => done.fail(err));
            });

    });

    afterEach(() => {
        consumerSubscription.unsubscribe();
        producerSubscription.unsubscribe();
    });

});
