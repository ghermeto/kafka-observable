/**
 * Extracts and deserialize an object from a JSON message
 *
 * @author ghermeto
 **/
'use strict';

const Observable = require('rxjs').Observable;
const kafkaMessage = require('./kafka-message');

const jsonMessage = (mapper = x => x) =>
    (source) =>
        Observable.create(observer =>
            source
                .let(kafkaMessage())
                .map(message => {
                    try { return JSON.parse(message); }
                    catch(err) { observer.error(err); }
                })
                .subscribe(
                    json => {
                        try { observer.next(mapper(json)); }
                        catch(err) { observer.error(err); }
                    },
                    err => observer.error(err),
                    () => observer.complete()));

module.exports = jsonMessage;