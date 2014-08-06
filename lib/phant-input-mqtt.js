/**
 * phant-input-mqtt
 * https://github.com/sparkfun/phant-input-mqtt
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var mosca = require('mosca'),
  redis = require('redis'),
  util = require('util'),
  events = require('events');

/**** Make PhantInput an event emitter ****/
util.inherits(PhantInput, events.EventEmitter);

/**** PhantInput prototype ****/
var app = PhantInput.prototype;

/**** Expose PhantInput ****/
exports = module.exports = PhantInput;

/**** Initialize a new PhantInput ****/
function PhantInput(config) {

  if (!(this instanceof PhantInput)) {
    return new PhantInput(config);
  }

  events.EventEmitter.call(this, config);
  util._extend(this, config || {});

  this.server = new mosca.Server({
    port: this.port,
    backend: {
      type: 'redis',
      redis: redis,
      db: 0,
      port: this.redis_port,
      return_buffers: true,
      host: this.redis_host
    }
  });

  this.server.on('published', this.incoming.bind(this));

}

/**** Defaults ****/
app.name = 'MQTT Input';
app.port = 1883;
app.redis_host = 'localhost';
app.redis_port = 6379;
app.keychain = false;
app.validator = false;
app.server = false;

/**** Default throttler ****/
app.throttler = {
  available: function(key, cb) {
    cb(true);
  }
};

app.incoming = function(packet) {

  var topic = packet.topic.split('/'),
      pub = topic[0],
      action = topic[1],
      prv = topic[2],
      input = this,
      id, data;

  try {
    data = JSON.parse(packet.payload);
  } catch (e) {
    return this.emit('error', e);
  }

  // check for public key
  if (!pub) {
    return;
  }

  // check for private key
  if (!prv) {
    return;
  }

  // validate keys
  if (!this.keychain.validate(pub, prv)) {
    return;
  }

  // get the id
  id = this.keychain.getIdFromPrivateKey(prv);

  if (action === 'clear') {
    return this.emit('clear', id);
  }

  // make sure they sent some data
  if (!data) {
    return;
  }

  // add timestamp
  data.timestamp = new Date().toISOString();

  this.throttler.available(pub, function(ready) {

    if (!ready) {
      return;
    }

    input.validator.fields(id, data, function(err, valid) {

      if (!valid) {
        return;
      }

      input.emit('data', id, data);

    });

  });

};
