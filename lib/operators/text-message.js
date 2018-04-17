/**
 * Observable operator: extracts a string value from a kafka message
 *
 * @example
 * const opts = { brokers: 'kafka://127.0.0.1:9092' };
 * const KafkaObservable = require('kafka-observable');
 * KafkaObservable.fromTopic('my_topic', opts)
 *     .textMessage()
 *     .subscribe(message => console.info(message));
 *
 * @module operators/TextMessageOperator
 * @author ghermeto
 **/
'use strict';

const { Subscriber } = require('rxjs');

class TextMessageOperator {

    /**
     * @constructor
     * @param {Function} mapper map function (defaults to identity)
     */
    constructor(mapper = x => x) {
        this.mapper = mapper;
    }

    /**
     * @override
     * @param {Subscriber} subscriber
     * @param {Observable} source
     */
    call(subscriber, source) {
        return source.subscribe(new MessageSubscriber(subscriber, this.mapper));
    }
}

/**
 * @private
 * @class MessageSubscriber
 */
class MessageSubscriber extends Subscriber {
    /**
     * @constructor
     * @param {Subscriber} subscriber
     * @param {Function} mapper map function
     */
    constructor(subscriber, mapper) {
        super(subscriber);
        this.mapper = mapper;
    }

    /**
     * @param {Buffer} message
     */
    next({ message }) {
        try { super.next(this.mapper(message.toString('utf8'))); }
        catch(err) { super.error(err); }
    }
}

module.exports = TextMessageOperator;