/**
 * Kafka Client Adapter - adapts the no-kafka client to the expected interface
 *
 * @example <caption>subscriber</caption>
 * const kafkaClient = require('./lib/client');
 * const consumer = kafkaClient.consumer({
 *      brokers: 'https://exemple.com:7123',
 *      groupId: 'audit',
 * });
 *
 * consumer.on('error', err => console.error(err));
 * consumer.subscribe('my_topic', (messageSet, topic, partition) => {
 *   messageSet.forEach(promise => promise.then(({offset, message}) => {
 *      console.info(topic, partition, message.value.toString('utf8'), offset);
 *      return consumer.client().commitOffset({t opic, partition, offset });
 *   }));
 * });
 *
 * @example <caption>producer</caption>
 * const kafkaClient = require('./lib/client');
 * const producer = kafkaClient.producer({brokers: 'https://exemple.com:7123'});
 *
 * const message = { hello: 'world', ok: true };
 * producer.on('error', err => console.error(err));
 * producer.publish('my_topic', message);
 *
 * @author ghermeto
 **/
'use strict';

const Producer = require('./producer');
const Consumer = require('./consumer');

exports.producer = opts => new Producer(opts);
exports.consumer = opts => new Consumer(opts);
