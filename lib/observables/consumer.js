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
 * @module observables/Consumer
 * @author ghermeto
 **/

const bunyan = require('bunyan');
const defaultFactory = require('../client');
const { Observable, Subscription } = require('rxjs');

const CommitOperator = require('../operators/commit');
const TextMessageOperator = require('../operators/text-message');

const subscriberError = 'Error executing subscriber. Message not committed.';

/**
 * logger
 * @constant
 * @type {Object}
 */
const log = bunyan.createLogger({
    name: 'KafkaObservable',
    level: process.env.LOG_LEVEL
});

/**
 * default consumer options
 * @constant
 * @type {Object}
 */
const defaults = {
    autoCommit: true
};

/**
 * Creates a selector function with the kafka consumer in scope
 *
 * @private
 * @param {Subscriber} subscriber
 * @param {Object | defaultFactory} kafkaConsumer
 */
const selectorFactory =
    (subscriber, kafkaConsumer) =>
        /**
         * Handles a kafka message. Will consume the message and pass it to
         * the observable
         *
         * @private
         * @param {Array} messageSet list of messages received
         * @param {String} topic topic which the message was received
         * @param {Number} partition partition on which the message was received
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
                        subscriber.next({ topic, partition, offset, message });
                        // must return commit as a promise
                        return kafkaConsumer.commit(topic, partition, offset);
                    } catch (err) {
                        log.error(subscriberError, err);
                    }
                }

                subscriber.next({ topic, partition, offset, message, commit });
            });

/**
 * Kafka Consumer Observable class
 * @class KafkaConsumerObservable
 */
class KafkaConsumerObservable extends Observable {

    /**
     * @constructor
     * @param {String} topic topic name
     * @param {Object} options
     * @param {Array|String} options.brokers comma separated list of brokers
     * @param {Object | defaultFactory} clientFactory
     * @return {Observable}
     * @see {@link https://github.com/oleksiyk/kafka/blob/master/README.md} for default client options.
     */
    constructor(topic, options, clientFactory = defaultFactory) {
        super();
        this.topic = topic;
        this.options = options;

        const opts = Object.assign({}, defaults, options);
        this._adapter = clientFactory.consumer(opts);
    }

    /**
     * Consumer factory.
     *
     * @param {String} topic topic name
     * @param {Object} options
     * @param {Array|String} options.brokers comma separated list of brokers
     * @param {Object | defaultFactory} clientFactory
     * @return {Observable}
     * @see {@link https://github.com/oleksiyk/kafka/blob/master/README.md} for default client options.
     */
    static create(topic, options, clientFactory) {
        return new KafkaConsumerObservable(topic, options, clientFactory);
    }

    /**
     * Overrides lift so that operators return an instance of this class.
     *
     * @override
     * @method lift
     * @param {Operator} operator
     * @return {KafkaConsumerObservable} new observable with operator applied
     */
    lift(operator) {
        const kcObservable = new KafkaConsumerObservable();
        kcObservable.source = this;
        kcObservable.operator = operator;
        return kcObservable;
    }

    /**
     * Returns kafka message as string
     * @param {Function} mapper map function (defaults to identity)
     * @return {KafkaConsumerObservable}
     */
    textMessage(mapper = x => x) {
        return this.lift(new TextMessageOperator(mapper));
    }

    /**
     * Commits message if options.autoCommit is false
     * @return {KafkaConsumerObservable}
     */
    commit() {
        return this.lift(new CommitOperator());
    }

    /**
     * @override
     * @protected
     * @param {Subscriber} subscriber
     * @returns {Subscription}
     */
    _subscribe(subscriber) {
        const { topic, _adapter: consumer } = this;

        consumer
            .subscribe(topic, selectorFactory(subscriber, consumer))
            .catch(err => subscriber.error(err));

        return new Subscription(() => consumer.end(topic));
    }
}

module.exports = KafkaConsumerObservable;