/**
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
 * @author ghermeto
 **/
'use strict';

const consumer = require('./lib/observables/consumer');
const producer = require('./lib/observables/producer');

const jsonMessage = require('./lib/operators/json-message');
const kafkaMessage = require('./lib/operators/kafka-message');

/**
 * @param {Object} options client options
 * @param {boolean} options.autoCommit automatically commits read message
 * @param {Object|String} options.partitioner name or class for partitioner implementation
 * @param {String} options.strategy name of the assignment strategy
 * @param {Object} clientFactory your custom kafka client adapter
 * @see {@link https://github.com/oleksiyk/kafka/blob/master/README.md} for default client options.
 */
function KafkaObservable(options, clientFactory) {
    return {
        fromTopic:
            (topic, opts = options, client = clientFactory) =>
                consumer.create(topic, opts, client),
        toTopic:
            (topic, messages, opts = options, client = clientFactory) =>
                producer.create(topic, messages, opts, client)
    }
}

// observables
KafkaObservable.fromTopic = KafkaObservable().fromTopic;
KafkaObservable.toTopic = KafkaObservable().toTopic;

// operators
KafkaObservable.JSONMessage = jsonMessage;
KafkaObservable.TextMessage = kafkaMessage;

module.exports = KafkaObservable;