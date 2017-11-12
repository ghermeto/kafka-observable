/**
 * Kafka Consumer Observable.
 *
 * @example <caption>consumer</caption>
 * const consumerObservable = require('./lib/consumer');
 * const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'me' };
 * const observable = consumerObservable.create('my_topic', opts);
 *
 * observable
 *   .do(m => m.commit())
 *   .map(m => m.message)
 *   .subscribe(message => console.info(message));
 *
 * @author ghermeto
 **/

const bunyan = require('bunyan');
const defaultFactory = require('../client');
const Observable = require('rxjs').Observable;

const observerError = 'Error executing observer. Message not committed.';

const log = bunyan.createLogger({
    name: 'KafkaObservable',
    level: process.env.LOG_LEVEL
});

/**
 * default consumer options
 * @type {{
 *   autoCommit: boolean
 * }}
 */
const defaults = {
    autoCommit: true
};

/**
 * Creates a selector function with the kafka consumer in scope
 *
 * @private
 * @param {Observer} observer
 * @param {Object | defaultFactory} kafkaConsumer
 */
const selectorFactory =
    (observer, kafkaConsumer) =>
        /**
         * Handles a kafka message. Will consume the message and pass it to
         * the observable
         *
         * @private
         * @param {Array} messageSet list of messages received
         * @param {String} topic topic which the message was received
         * @param {Number} partition partion on which the message was received
         */
        (messageSet, topic, partition) =>
            messageSet.map(({offset, message}) => {
                const value = message.value;
                const decoded = value ? value.toString('utf8') : null;
                log.trace(topic, partition, offset, decoded);

                // pre-wired commit function as observable
                const commit = () =>
                    Observable
                        .fromPromise(kafkaConsumer
                            .commit(topic, partition, offset));

                if (kafkaConsumer.opts.autoCommit) {
                    // if we will commit here, no need to return commit function
                    try {
                        observer.next({ topic, partition, offset, message });
                        // must return commit as a promise
                        return kafkaConsumer.commit(topic, partition, offset);
                    } catch (err) {
                        log.error(observerError, err);
                    }
                }

                observer.next({ topic, partition, offset, message, commit });
            });

/**
 * Observable as result from messages in a kafka topic
 *
 * @private
 * @param {Object} consumer
 * @param {String} topic kafka topic
 * @return {Observable}
 */
const fromTopic = (consumer, topic) => {
    const observable = Observable.create(observer => {
        consumer
            .subscribe(topic, selectorFactory(observer, consumer))
            .catch(err => observer.error(err));

        return () => consumer.end(topic);
    });

    observable._adapter = consumer;
    return observable;
};

/**
 * Consumer factory.
 *
 * @param {String} topic topic name
 * @param {Object} options
 * @param {Array|String} options.brokers comma separated list of brokers
 * @param {Object | defaultFactory} clientFactory
 * @return {Observable}
 */
exports.create = function (topic, options, clientFactory = defaultFactory) {
    const opts = Object.assign({}, defaults, options);
    try {
        const kafkaConsumer = clientFactory.consumer(opts);
        return fromTopic(kafkaConsumer, topic);
    } catch (err) {
        return Observable.throw(err);
    }
};
