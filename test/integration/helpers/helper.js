'use strict';

const Docker = require('dockerode');
jasmine.DEFAULT_TIMEOUT_INTERVAL = 15000;

const topic = 'test_kafka';
const createTopic = () => ([
    '/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh',
    '--create',
    '--zookeeper', 'localhost:2181',
    '--replication-factor', '1',
    '--partitions', '1',
    '--topic', topic
]);

/**
 * @type {Docker}
 */
global.docker = new Docker();

/**
 * end handler for jasmine
 * @param {Function} done jasmine done function
 * @return {Function} handler that calls done the expected way
 **/
global.finish_test = function finish_test(done) {
    return function (err) {
        if (err) { done.fail(err); }
        done();
    };
};

/**
 * delay func
 * @param {Number} millis time in milliseconds
 * @return {Promise} resolves after given time
 */
global.delay = millis =>
    new Promise((resolve) => setTimeout(()=> resolve(), millis));

/**
 * Before integration tests starts, create the container, start it and create
 * the topic in kafka required for testing.
 */
beforeAll(function (done) {
    docker.createContainer({
        Image: 'spotify/kafka',
        AttachStdout: true,
        AttachStderr: true,
        Hostname: 'kafka',
        StopTimeout: 5,
        ExposedPorts: { '9092/tcp':{} },
        HostConfig: { PortBindings: { '9092/tcp': [{ HostPort: '9092' }] } }
    })
    .then(container => container.start())
    .then(container => global.container = container)
    .then(() => console.info('waiting for kafka to start...') || delay(5000))
    .then(() => container.exec({ Cmd: createTopic() }))
    .then(exec => exec.start())
    .then(() => console.info(`${topic} topic created...`) || done());
});

/**
 * test to see if container was successfully started
 */
it('should have a valid container', done => {
    container.inspect(function (err, data) {
        if (err || !data) { done.fail(err || 'no container data'); }
        done();
    });
});

/**
 * shutdown procedure. We need to stop and remove the container not only on
 * afterAll, but anytime we stop test tests.
 */
const gracefulShutdown = () => global.container
    .stop()
    .then(container => container.remove());

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

afterAll(done => gracefulShutdown().then(done));
