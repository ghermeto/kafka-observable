/**
 * Extracts a string value from a kafka message
 *
 * @author ghermeto
 **/
'use strict';

const Observable = require('rxjs').Observable;

const kafkaMessage = (mapper = x => x) =>
    (source) =>
        Observable.create(observer =>
            source
                .map(({message}) => message.value.toString('utf8'))
                .subscribe(
                    message => {
                        try { observer.next(mapper(message)); }
                        catch(err) { observer.error(err); }
                    },
                    err => observer.error(err),
                    () => observer.complete()));

module.exports = kafkaMessage;