/**
 * phant-input-mqtt
 * https://github.com/sparkfun/phant-input-mqtt
 *
 * Copyright (c) 2014 SparkFun Electronics
 * Licensed under the GPL v3 license.
 */

'use strict';

/**** Module dependencies ****/
var server = require('phant-server-mqtt'),
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

  this.server = server.create(this.port);

  this.server.on('published', this.incoming.bind(this));

}

/**** Defaults ****/
app.name = 'MQTT Input';
app.port = 1883;
app.keychain = false;
app.validator = false;
app.server = false;

/**** Default throttler ****/
app.throttler = {
  available: function(key, cb) {
    var now = Math.round((new Date()).getTime() / 1000);

    cb(true, 0, 100, now);
  }
};

app.incoming = function(packet) {

  if(! /^streams/.test(packet.topic)) {
    return;
  }

  var topic = packet.topic.split('/'),
      pub = topic[1],
      action = topic[2],
      prv = topic[3],
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

  if(action !== 'clear' && action !== 'input') {
    return;
  }

  // check for private key
  if (!prv) {
    return this.respond(pub, {
      success: false,
      message: 'Missing private key'
    });
  }

  // validate keys
  if (!this.keychain.validate(pub, prv)) {
    return this.respond(pub, {
      success: false,
      message: 'Invalid keys'
    });
  }

  // get the id
  id = this.keychain.getIdFromPrivateKey(prv);

  if(action === 'clear') {
    this.emit('clear', id);
    return this.respond(pub, {
      success: true,
      message: 'Stream cleared'
    });
  }

  // make sure they sent data
  if (!data) {
    return this.respond(pub, {
      success: false,
      message: 'No data sent'
    });
  }

  // add timestamp
  data.timestamp = new Date().toISOString();

  this.throttler.available(pub, function(ready, used, limit, reset) {

    if (!ready) {
      return input.respond(pub, {
        success: false,
        message: 'Rate limit exceeded',
        rate_used: used,
        rate_limit: limit,
        rate_reset: reset
      });
    }

    input.validator.fields(id, data, function(err, valid) {

      if (!valid) {
        return input.respond(pub, {
          success: false,
          message: err,
          rate_used: used,
          rate_limit: limit,
          rate_reset: reset
        });
      }

      input.emit('data', id, data);

      input.respond(pub, {
        success: true,
        message: 'Success',
        rate_used: used,
        rate_limit: limit,
        rate_reset: reset
      });

    });

  });

};

app.respond = function(pub, payload) {

  var message = {
    topic: 'streams/' + pub + '/response',
    payload: JSON.stringify(payload),
    qos: 0,
    retain: false
  };

  this.server.publish(message);

}
