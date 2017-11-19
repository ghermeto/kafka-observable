/**
 * Kafka Producer Observable.
 *
 * @example <caption>producer</caption>
 * const producerObservable = require('./lib/producer');
 * const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'me' };
 * const observable = producerObservable.create('my_topic', 'message', opts);
 *
 * observable.subscribe(result => console.info(result));
 *
 * @example <caption>producer - using arrays</caption>
 * const producerObservable = require('./lib/producer');
 * const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'me' };
 * const messages = ['first message', 'second message'];
 * const observable = producerObservable.create('my_topic', messages, opts);
 *
 * observable.subscribe(result => console.info(result));
 *
 * @example <caption>producer - using observables</caption>
 * const producerObservable = require('./lib/producer');
 * const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'me' };
 * const messages = Observable.from(['first message', 'second message']);
 * const observable = producerObservable.create('my_topic', messages, opts);
 *
 * observable.subscribe(result => console.info(result));
 *
 * @module observables/Producer
 * @author ghermeto
 **/

const defaultFactory = require('../client');
const Observable = require('rxjs').Observable;

/**
 * Makes sure messages is formatted as an array or an observable
 *
 * @private
 * @param {*} messages
 * @return {Observable|Array}
 */
const formatMessages =
        messages =>
            (messages instanceof Observable) ? messages : [].concat(messages);

/**
 * Observable as result of publishing messages to Kafka
 *
 * @private
 * @param {String} topic
 * @param {Array|Observable} messages
 * @param {Object} producer
 */
const toTopic =
    (topic, messages, producer) => {
        const observable = Observable.create(observer => {
            Observable.from(messages)
            // formats and sends the message through kafka
                .concatMap(message => {
                    const {value = message, key} = message;
                    const result = producer.publish(topic, value, key);
                    return Observable.fromPromise(result);
                })
                // removes surrounding array and flattens
                .concatMap(result => Observable.from(result))
                .catch(err => observer.error(err))
                .subscribe({
                    next: result => observer.next(result),
                    error: err => observer.error(err),
                    complete: () => observer.complete()
                });
            return () => {
                return producer.end();
            };
        });

        observable._adapter = producer;
        return observable;
    };

/**
 * Producer factory.
 *
 * @param {String} topic topic name
 * @param {Array|Observable} messages
 * @param {Object} options
 * @param {Array|String} options.brokers comma separated list of brokers
 * @param {Object|defaultFactory} clientFactory client factory
 * @return {Observable}
 * @see {@link https://github.com/oleksiyk/kafka/blob/master/README.md} for default client options.
 */
exports.create = (topic, messages, options, clientFactory = defaultFactory) => {
    try {
        const kafkaProducer = clientFactory.producer(options);
        return toTopic(topic, formatMessages(messages), kafkaProducer);
    } catch (err) {
        return Observable.throw(err);
    }
};
