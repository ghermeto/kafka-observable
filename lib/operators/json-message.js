/**
 * Observable operator: extracts and deserialize an object from a JSON message.
 *
 * @example
 * const opts = { brokers: 'kafka://127.0.0.1:9092' };
 * const KafkaObservable = require('kafka-observable')(opts);
 * KafkaObservable.fromTopic('my_topic')
 *     .let(KafkaObservable.JSONMessage())
 *     .subscribe(message => console.info(message));
 *
 * @module operators/JSONMessage
 * @author ghermeto
 **/
'use strict';

const Observable = require('rxjs').Observable;
const kafkaMessage = require('./kafka-message');

const jsonMessage = (mapper = x => x) =>
    (source) =>
        Observable.create(observer =>
            source
                .let(kafkaMessage())
                .map(message => {
                    try { return JSON.parse(message); }
                    catch(err) { observer.error(err); }
                })
                .subscribe(
                    json => {
                        try { observer.next(mapper(json)); }
                        catch(err) { observer.error(err); }
                    },
                    err => observer.error(err),
                    () => observer.complete()));

/**
 * @function JSONMessage
 * @param {Function} mapper mapping function
 */
module.exports = jsonMessage;