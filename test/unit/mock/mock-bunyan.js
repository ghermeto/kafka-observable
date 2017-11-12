/**
 * mocks no-kafka module
 * @author ghermeto
 * @version 0.0.1
 **/
const mock = require('mock-require');

const bunyan = {
    log: () => {},
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
};

mock('bunyan', { createLogger: () => bunyan });
mock.reRequire('bunyan');

module.exports = { bunyan };
beforeAll(() => {
    global.bunyan = bunyan;
});
