[![node](https://img.shields.io/node/v/kafka-observable.svg)]()
[![Build Status][badge-travis]][travis]
[![Test Coverage][badge-coverage]][coverage]
[![license][badge-license]][license]

# kafka-observable

__kafka-observable__ is the easiest way to exchange messages through kafka with [node.js](https://nodejs.org).


Using the solid [no-kafka](https://github.com/oleksiyk/kafka) as default client, __kafka-observable__ creates [RxJS](https://github.com/ReactiveX/rxjs) observables 
that can be manipulated as if you were using Kafka Streams, but with a familiar interface 
to javascript developers.

### Why observables?

Think of observables as collections which elements arrive over time. You may iterate over 
them, which means you can also apply filter, map or reduce. 

Many of the operations provided by observables are very similar to the capabilities available 
in Kafka Streams, including the ability to use window (accumulate values for a period).

### Installation

```bash
npm install --save kafka-observable
```

### Example usage

Imagine your customers can subscribe to out-of-stock products in your online store to 
receive emails when the product is in stock. Your stock management publishes updates
to a kafka topic called __inventory_updates__ and you mailer consumes from __notifications__.

```javascript
const opts = { 
    brokers: 'kafka://kafka-broker.example.com:9092', 
    groupId: 'inventory-notifications' 
};

const KafkaObservable = require('kafka-observable')(opts);
// assumes getWatchers will execute a network request and returns an observable
const getWatchers = require('./lib/observables/watchers');

const subscription = KafkaObservable.fromTopic('inventory_updates')
    // gets messge as JSON
    .let(KafkaObservable.JSONMessage())
    // just arrived
    .filter(({inventory}) => inventory.previous === 0 && inventory.current > 0)
    // gets watchers, format message and concat 2 dimensional observable
    .concatMap(product => 
        getWatchers(product.id)
            .map(watchers => ({ watchers, product }))
    // sends formated message to new topic and concats 2 dimensional observable
    .concatMap(message => KafkaObservable.toTopic('notifications', message));

subscription.subscribe(success => console.log(success), err => console.error(err));    
``` 

## Methods

### fromTopic(topic, options, adapterFactory = defaultAdapterFactory)

Creates an observable that will consume from a Kafka topic.

#### Parameters
* topic (String) - topic name
* options (Object) - client options
* adapterFactory (Object) - client adapter (Defaults to no-kafka adapter)

#### Examples

KafkaObservable as a function:
```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'test' };

const KafkaObservable = require('kafka-observable')(opts);
const consumer = KafkaObservable.fromTopic('my_topic')
    .map(({message}) => message.value.toString('utf8'));

consumer.subscribe(message => console.info(message));
```

Passing __options__ parameter to fromTopic:
```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'test' };

const KafkaObservable = require('kafka-observable');
const consumer = KafkaObservable.fromTopic('my_topic', opts)
    .map(({message}) => message.value.toString('utf8'));

consumer.subscribe(message => console.info(message));
```

#### Options

Below are the main options for the consumer. For more consumer options, please
refer to [no-kafka](https://github.com/oleksiyk/kafka) options (in case you use the provided default adapter).

| Option | Required | Type | Default | Description |
|---|---|---|---|---|
| brokers | yes | Array/String | - | list of Kafka brokers 
| groupId | yes | String| - | consumer group id  
| autoCommit | no | boolean |true | commits the message offset automatically if no exception is thrown
| strategy | no | String | Default | name of the assignment strategy for the consumer (Default/Consistent/WeightedRoundRobin)

### toTopic(topic, messages, options, adapterFactory = defaultAdapterFactory)

Creates an observable that publishes messages to a Kafka topic.

#### Parameters
* topic (String) - topic name
* messages (String|Array|Observable) - messages to be published in kafka topic
* options (Object) - client options
* adapterFactory (Object) - client adapter (Defaults to no-kafka adapter)

#### Examples

KafkaObservable as a function:
```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092' };
const messages = [{key: 'value1'}, {key: 'value2'}];

const KafkaObservable = require('kafka-observable')(opts);
const producer = KafkaObservable.toTopic('my_topic', messages);

producer.subscribe(message => console.info(message));
```

Passing __options__ parameter to toTopic:
```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092' };
const messages = Observable.from([{key: 'value1'}, {key: 'value2'}]);

const KafkaObservable = require('kafka-observable');
const producer = KafkaObservable.toTopic('my_topic', messages, opts);

producer.subscribe(message => console.info(message));
```

#### Options

Below are the main options for the producer. For more producer options, please
refer to [no-kafka](https://github.com/oleksiyk/kafka) options (in case you use the provided default adapter).

| Option | Required | Type | Default | Description |
|---|---|---|---|---|
| brokers | yes | Array/String | - | list of Kafka brokers 
| partitioner | no | prototype/String | Default | name (Default/HashCRC32) or prototype (instance of Kafka.DefaultPartitioner) to use as producer partitioner

### TextMessage(mapper = (x) => x)

Convenience operator which converts a Buffer message value into utf8 string.

#### Parameters
* mapper (Function) - mapper function

#### Example

```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'test' };

const KafkaObservable = require('kafka-observable');
const consumer = KafkaObservable.fromTopic('my_topic', opts)
    .let(KafkaObservable.TextMessage());

consumer.subscribe(message => console.info(message));
```

### JSONMessage(mapper = (x) => x)

Convenience operator provided to deserialize an object from a JSON message.

#### Parameters
* mapper (Function) - mapper function

#### Example

```javascript
const opts = { brokers: 'kafka://127.0.0.1:9092', groupId: 'test' };

const KafkaObservable = require('kafka-observable');
const consumer = KafkaObservable.fromTopic('my_topic', opts)
    .let(KafkaObservable.JSONMessage());

consumer.subscribe(json => console.info(json.key));
```

## Custom Kafka Adapter

If you don't want to use [no-kafka](https://github.com/oleksiyk/kafka) you can write an adapter for your client which respects
the interface established by the code in `lib/client`.

#### Why an adapter? 

I currently use an internal kafka client at Netflix with an interface very similar to this adapter 
and I wanted it to work out-of-the-box.

## Development

#### Unit tests
```bash
npm install
npm run unit-test
``` 

#### Integration tests
*requires docker to be installed and accessible through the **docker** command*
```bash
npm install
docker pull spotify/kafka
npm run unit-test
``` 

#### Test coverage
*based on unit tests*
```bash
npm install
npm run coverage
open coverage/lcov-report/index.html 
``` 

#### Documentation
```bash
npm install
npm run gen-docs
open out/index.html 
``` 
___
### License: [MIT](https://github.com/ghermeto/kafka-observable/blob/master/LICENSE)

[badge-license]: https://img.shields.io/badge/License-MIT-green.svg
[license]: https://github.com/ghermeto/kafka-observable/blob/master/LICENSE
[badge-travis]: https://api.travis-ci.org/ghermeto/kafka-observable.svg?branch=master
[travis]: https://travis-ci.org/ghermeto/kafka-observable
[badge-coverage]: https://codeclimate.com/github/ghermeto/kafka-observable/badges/coverage.svg
[coverage]: https://codeclimate.com/github/ghermeto/kafka-observable/coverage
 