/**
 * Observable operator: commits a kafka message
 *
 * @example
 * const opts = { brokers: 'kafka://127.0.0.1:9092' };
 * const KafkaObservable = require('kafka-observable');
 * KafkaObservable.fromTopic('my_topic', opts)
 *     .commit()
 *     .textMessage()
 *     .subscribe(message => console.info(message));
 *
 * @module operators/CommitOperator
 * @author ghermeto
 **/
'use strict';

const { Subscriber } = require('rxjs');

class CommitOperator {

    /**
     * @constructor
     * @param {Consumer} consumer kafka consumer client
     */
    constructor(consumer) {
        this.consumer = consumer;
    }

    /**
     * @override
     * @param {Subscriber} subscriber
     * @param {Observable} source
     */
    call(subscriber, source) {
        return source.subscribe(new CommitSubscriber(subscriber, this.consumer));
    }
}

/**
 * @private
 * @class CommitSubscriber
 */
class CommitSubscriber extends Subscriber {
    /**
     * @constructor
     * @param {Subscriber} subscriber
     * @param {Consumer} consumer kafka consumer client
     */
    constructor(subscriber, consumer) {
        super(subscriber);
        this.consumer = consumer;
    }

    /**
     * @override
     * @param {String} topic topic which the message was received
     * @param {Number} partition partition on which the message was received
     * @param {Number} offset message offset
     * @param {Buffer} message
     * @param {Function} commit
     */
    next({ topic, partition, offset, message, commit }) {
        const passthrough = { topic, partition, offset, message };

        if (this.consumer.opts.autoCommit) {
            return super.next(passthrough);
        }

        commit().subscribe(() => super.next(passthrough));
    }
}

module.exports = CommitOperator;