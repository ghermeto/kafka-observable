/**
 * Creates kafka consumer and producer observables given a kafka client adapter
 * and a options object.
 *
 * @example <caption>consumer</caption>
 * const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'me' };
 * const KafkaObservable = require('kafka-observable')(opts);
 * const observable = KafkaObservable.fromTopic('my_topic');
 *
 * @example <caption>producer</caption>
 * const opts = { brokers: 'kafka://127.0.0.1:9092' };
 * const KafkaObservable = require('kafka-observable')(opts);
 * const observable = KafkaObservable.toTopic('my_topic', 'my message');
 *
 * @module KafkaObservable
 * @author ghermeto
 **/
'use strict';

const Consumer = require('./lib/observables/consumer');
const Producer = require('./lib/observables/producer');

const jsonMessage = require('./lib/operators/json-message');
const kafkaMessage = require('./lib/operators/kafka-message');

/**
 * @param {Object} options client options
 * @param {boolean} options.autoCommit automatically commits read message
 * @param {Object|String} options.partitioner name or class for partitioner implementation
 * @param {String} options.strategy name of the assignment strategy
 * @param {Object} clientFactory your custom kafka client adapter
 * @returns {ObservableFactory}
 * @see {@link https://github.com/oleksiyk/kafka/blob/master/README.md} for default client options.
 */
function KafkaObservable(options, clientFactory) {
    /**
     * Object capable of creating both the consumer and producer observables
     * @inner
     * @name ObservableFactory
     * @typedef {Object} ObservableFactory
     * @property {Function} fromTopic same as fromTopic from prototype
     * @property {Function} toTopic same as toTopic from prototype
     */
    return {
        /**
         * Creates a Consumer observable
         * @param {string} topic topic to subscribe
         * @param {Object} opts kafka client options
         * @param {Object} client kafka adapter
         * @returns {Observable}
         * @see {@link observables/Consumer}
         */
        fromTopic:
            (topic, opts = options, client = clientFactory) =>
                Consumer.create(topic, opts, client),
        /**
         * Creates a Producer observable
         * @param {string} topic topic to subscribe
         * @param {Array|Observable} messages message(s) to be sent
         * @param {Object} opts kafka client options
         * @param {Object} client kafka adapter
         * @returns {Observable}
         * @see {@link observables/Producer}
         */
        toTopic:
            (topic, messages, opts = options, client = clientFactory) =>
                Producer.create(topic, messages, opts, client)
    }
}

// observables
KafkaObservable.fromTopic = KafkaObservable().fromTopic;
KafkaObservable.toTopic = KafkaObservable().toTopic;

// operators
KafkaObservable.JSONMessage = jsonMessage;
KafkaObservable.TextMessage = kafkaMessage;

module.exports = KafkaObservable;