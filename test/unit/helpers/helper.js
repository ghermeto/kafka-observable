'use strict';

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
