/**
 * Base Adapter class.
 *
 * @author ghermeto
 **/

const EventEmitter = require('events');
const bunyan = require('bunyan');
const shortid = require('shortid');

/**
 * Base pipeline class extended by both Consumer and Producer classes
 * @class Base
 */
class Base extends EventEmitter {

    constructor(opts) {
        super();
        this.isValid(opts);
        this.opts = opts;
        this.clientId = opts.clientId || `kafka-observable-${shortid.generate()}`;
        this.brokers = Base.formatBrokers(opts.brokers);

        this.log = bunyan.createLogger({
            name: this.clientId,
            level: process.env.LOG_LEVEL
        });
    }

    /**
     * Convert to array or throw if parameter is not string or array
     * @param {String|Array} brokers single or multiple brokers URL
     * @return {Array.<*>}
     */
    static formatBrokers(brokers) {
        const className = brokers.constructor.name;
        if (className !== 'Array' && className !== 'String') {
            throw new TypeError('invalid brokers parameter');
        }
        return [].concat(brokers);
    }

    /**
     * Validate options. Throws if invalid.
     * @param {Object} opts user defined options
     */
    isValid(opts) {
        if (!opts.brokers) {
            throw new Error('missing vipaddress or brokers parameter');
        }
    }

    /**
     * To be overridden by consumer and producer defaults
     * @return {Object} default client options
     */
    defaults() {
        return {
            connectionString: this.brokers.join(','),
            logger: {logFunction: this.logging.bind(this)}
        };
    }

    /**
     * @return {Object} client options
     */
    clientOptions() {
        return Object.assign({}, this.defaults(), this.opts);
    }

    /**
     * Emits error event if listener error is found.
     * EventEmitter throws if an error event is emitted without listeners
     * @see https://nodejs.org/api/events.html#events_error_events
     * @param {Object} err error object
     */
    handleError(err) {
        this.log.error(err);
        if (this.listenerCount('error') > 0) {
            this.emit('error', err);
        }
        return Promise.reject(err);
    }

    /**
     * Pipes logs through Bunyan and fire events
     * @param {String} level log level
     * @param {Array} args
     */
    logging(level, ...args) {
        this.log[level.toLowerCase()](...args);
        this.monitoring(level, ...args);
    }

    /**
     * Parses through the logs to emit meaningful events
     * @param {String} level log level
     * @param {Array} args
     */
    monitoring(level, ...args) {
        if (level === bunyan.ERROR) {
            this.handleError(args[3]);
        }
    }

}

module.exports = Base;