/**
 * Observable operator: extracts a string value from a kafka message
 *
 * @example
 * const opts = { brokers: 'kafka://127.0.0.1:9092' };
 * const KafkaObservable = require('kafka-observable')(opts);
 * KafkaObservable.fromTopic('my_topic')
 *     .let(KafkaObservable.TextMessage())
 *     .subscribe(message => console.info(message));
 *
 * @module operators/TextMessage
 * @author ghermeto
 **/
'use strict';

const Observable = require('rxjs').Observable;

const kafkaMessage = (mapper = x => x) =>
    (source) =>
        Observable.create(observer =>
            source
                .map(({message}) => message.value.toString('utf8'))
                .subscribe(
                    message => {
                        try { observer.next(mapper(message)); }
                        catch(err) { observer.error(err); }
                    },
                    err => observer.error(err),
                    () => observer.complete()));

/**
 * @function TextMessage
 * @param {Function} mapper mapping function
 */
module.exports = kafkaMessage;