/**
 * Kafka Consumer Adapter. It adapts no-kafka group consumer to fits the
 * KafkaObservable interface.
 *
 * @example <caption>consumer</caption>
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
 * @author ghermeto
 **/
const Kafka = require('no-kafka');
const Base = require('./base');

/**
 * Enables subscribing to a topic in the pipeline
 * @class Consumer
 */
class Consumer extends Base {
    constructor(opts) {
        super(opts);
    }

    /**
     * Validates required options
     * @private
     * @override
     * @param {Object} opts user defined options
     */
    isValid(opts) {
        super.isValid(opts);
        if (!opts.groupId) { throw new Error('missing consumer groupId'); }
    }

    /**
     * Dynamically generates consumer default configuration properties
     * @private
     * @override
     * @returns {Object} Default configs
     */
    defaults() {
        const base = super.defaults();
        return Object.assign({
            idleTimeout: 100,
            heartbeatTimeout: 100,
            clientId: this.clientId
        }, base);
    }

    /**
     * Returns the consumer client once it has subscribed to a topic.
     *
     * @return {Kafka.GroupConsumer|GroupConsumer}
     */
    client() {
        return this.consumer;
    }

    /**
     * Convert strategy name to object.
     * Possible values are: WeightedRoundRobin, Consistent and Default.
     * @private
     * @param {String} name strategy name.
     */
    strategy(name = 'Default') {
        const className = `${name}AssignmentStrategy`;
        if (!Kafka[className]) {
            this.log.warn(`Strategy ${className} not found. Using default.`);
            return new Kafka.DefaultAssignmentStrategy();
        }
        return new Kafka[className]();
    }

    /**
     * First unsubscribe from the topic and then ends the client connection.
     *
     * @param {String} topic topic to unsubscribe
     * @return {Promise}
     */
    end(topic) {
        return this.client()
            .unsubscribe(topic)
            .then(() => this.client().end());
    }

    /**
     * Commits the received message
     *
     * @param {String} topic topic which had the message
     * @param {Number} partition partition which had the message
     * @param {Number} offset message offset in the topic partition
     * @return {Promise}
     */
    commit(topic, partition, offset) {
        return this.client().commitOffset({ topic, partition, offset });
    }

    /**
     * Subscribe to a list of channels passing a callback
     *
     * @param {Array} topics topics to subscribe
     * @param {Function} handler function to be executed when a message arrives
     */
    subscribe(topics, handler) {
        const options = this.clientOptions();
        this.consumer = new Kafka.GroupConsumer(options);

        const strategies = [{
            strategy: this.strategy(this.opts.strategy),
            metadata: this.opts.metadata,
            subscriptions: [].concat(topics),
            handler
        }];

        return this.consumer.init(strategies)
            .catch(err => this.handleError(err));
    }
}

module.exports = Consumer;