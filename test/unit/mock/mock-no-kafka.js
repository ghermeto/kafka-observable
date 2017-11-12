/**
 * mocks no-kafka module
 * @author ghermeto
 * @version 0.0.1
 **/
const mock = require('mock-require');
const shortid = require('shortid');

const mockKafka = {
    offset: 0,
    partition: 0,
    registry: {}
};

class GroupConsumer {
    constructor(options) {
        this.options = options;
    }

    init(strategies) {
        const metadata = strategies[0].metadata;
        if (metadata && metadata !== 'object') {
            return Promise.reject();
        }

        this.strategies = strategies;
        strategies.forEach(strategy => {

            mockKafka.registry = strategy.subscriptions
                .reduce((registry, topic) => {
                    const current = registry[topic] || [];
                    const updatedList = current.concat(strategy.handler);
                    return Object.assign({}, registry, {[topic]: updatedList});
                }, mockKafka.registry);
        });
        return Promise.resolve(mockKafka.offset);
    }

    commitOffset() {
        return Promise.resolve(mockKafka.offset++);
    }

    unsubscribe(topic) {
        delete mockKafka.registry[topic];
        return Promise.resolve();
    }

    end() {
        this.strategies = null;
        mockKafka.registry = {};
    }
}

class Producer {
    constructor(options) {
        this.options = options;
        options.logger.logFunction('trace', ['mock']);
    }

    init() {
        return Promise.resolve();
    }

    end() {
        return Promise.resolve();
    }

    send({ topic, message }) {
        const error = this.options.metadata ? new Error('test') : null;
        mockProducer().publish(topic, message.value, message.key);
        return Promise.resolve([{ topic, error }]);
    }
}

const mockProducer = () => {
    return {
        publish(topic, message, key = shortid.generate()) {
            const handlerList = mockKafka.registry[topic];
            if (!mockKafka.registry[topic]) {
                throw 'No consumer in such topic';
            }

            const msg = this.createMessage(message, key);

            handlerList.forEach(handler => {
                handler([msg], topic, mockKafka.partition);
            });
        },

        createMessage(message, key) {
            return {
                offset: mockKafka.offset,
                messageSize: message ? message.length : 0,
                message: {
                    key: key ? Buffer.from(key, 'utf8') : null,
                    value: message
                        ? typeof message === 'string'
                            ? Buffer.from(message, 'utf8')
                            : message
                        : null
                }
            }
        }

    }
};

const mockKafkaClient = {
    DefaultAssignmentStrategy: class DefaultAssignmentStrategy { },
    DefaultPartitioner: class DefaultPartitioner { },
    GroupConsumer: GroupConsumer,
    Producer: Producer
};

mock('no-kafka', mockKafkaClient);
mock.reRequire('no-kafka');

module.exports = { mockKafkaClient, mockProducer };
beforeAll(() => {
    global.kafkaClient = mockKafkaClient;
    global.mockProducer = mockProducer;
});
