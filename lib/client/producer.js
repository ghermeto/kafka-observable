/**
 * Kafka Producer Adapter. It adapts no-kafka producer to the format expected
 * by the KafkaObservable interface
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

const Kafka = require('no-kafka');
const Base = require('./base');
const shortid = require('shortid');

/**
 * @class Producer
 */
class Producer extends Base {
    constructor(opts) {
        super(opts);
        const options = this.clientOptions();
        this.producer = new Kafka.Producer(Object.assign({}, options));
    }

    /**
     * @return {Object} client options
     */
    clientOptions() {
        const base = super.clientOptions();
        const fixed = { partitioner: this.partitioner(this.opts.partitioner) };
        return Object.assign({}, base, fixed);
    }

    /**
     * Dynamically generates producer default configuration properties
     * @private
     * @override
     * @returns {Object} Default configs
     */
    defaults() {
        const base = super.defaults();
        const defaults = { timeout: 1000 };
        return Object.assign({}, base, defaults);
    }

    /**
     * Initializes the producer if necessary or use the cached producer
     * @private
     * @return {Promise} active producer
     */
    init() {
        if (this.active) { return this.active; }
        this.active = this.producer.init();
        return this.active;
    }

    /**
     * Returns a partitioner instance from a string or from a class.
     * @private
     * @param {String|Object} nameOrClass name of the partitioner or class
     * @return {Object} partitioner instance
     */
    partitioner(nameOrClass = 'Default') {
        if (typeof nameOrClass === 'object') {
            return new nameOrClass();
        }
        const className = `${nameOrClass}Partitioner`;
        if (!Kafka[className]) {
            this.log.warn(`Partitioner ${className} not found. Using default.`);
            return new Kafka.DefaultPartitioner();
        }
        return new Kafka[className]();
    }

    /**
     * Serializes messages
     * @static
     * @private
     * @param {Object|String} message
     * @return {String} serialized message
     */
    static serialize(message) {
        return typeof message === 'string' ? message : JSON.stringify(message);
    }

    /**
     * Returns the producer client once it has sent at least one message.
     * @return {Kafka.Producer|Producer}
     */
    client() {
        return this.producer;
    }

    /**
     * Terminates the client connection.
     * @return {Promise}
     */
    end() {
        return this.client().end();
    }

    /**
     * Publishes a message to Kafka
     * @param {String} topic topic to publish on
     * @param {Object|String} message message to be sent
     * @param {String} key message key
     * @return {Promise}
     */
    publish(topic, message, key = `${this.clientId}-${shortid.generate()}`) {
        const serialized = Producer.serialize(message);
        const payload = {topic: topic, message: {key: key, value: serialized}};

        return this.init()
            .then(() => this.producer.send(payload))
            .catch(err => this.handleError(err));
    }
}

module.exports = Producer;