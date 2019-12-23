// Sqlite Storage Plugin
// Copyright (c) 2015 - 2018 Joseph Huckaby
// Released under the MIT License

// Requires the 'sqlite3' module from npm
// npm install sqlite3

var Class = require("pixl-class");
var Component = require("pixl-server/component");
var Tools = require("pixl-tools");
var sqlite3 = require('sqlite3');

module.exports = Class.create({
	__name: 'Sqlite',
	__parent: Component,
	
	defaultConfig: {
		connectString: ":memory:",
		keyPrefix: "",
		flushWalMinutes: 1
	},
	
	startup: function(callback) {
		var self = this;
		this.setup(callback);
		// this.config.on('reload', function() { self.setup(); } );
	},
	
	setup: function(callback) {
		// setup Couchbase connection
		var self = this;
		var cstr = this.config.get('connectString');
		self.logDebug(9, "connectString value => " + cstr);
		self.keyPrefix = this.config.get('keyPrefix')+"";
		self.db = new sqlite3.Database(cstr);
		self.flushWalMinutes = parseFloat(this.config.get('flushWalMinutes'));
		
		if(isNaN(self.flushWalMinutes)) self.flushWalMinutes = 0;
		self.lastFlush = new Date(0);
		
		self.db.run("PRAGMA journal_mode=WAL;");
		self.db.run("CREATE TABLE IF NOT EXISTS kv(key TEXT NOT NULL, val BLOB, lastmod int, size int, PRIMARY KEY (key))", (e) => {
			callback(e);
			self.logDebug(9, "setup completed");
		});
	},
	prepKey: function(key) {
		// prepare key for S3 based on config
		if (this.keyPrefix && this.keyPrefix != "") {
			key = this.keyPrefix + key;
		}
		return key;
	},
	
	put: function(key, value, callback) {
		var self = this;
		//self.logDebug(9, "put key=" + key + " value=" + JSON.stringify(value));
		key = self.prepKey(key);
		var sz = null;
		if(self.storage.isBinaryKey(key)) {
			sz = value.length;
		}
		else {
			value = JSON.stringify(value)
		}

		var stmt_put = self.db.prepare("INSERT OR REPLACE INTO KV(key,val,lastmod,size)VALUES(?,?,?,?)");
		stmt_put.run([key, value, (new Date()).getTime(), sz], function(e) {
			stmt_put.finalize();
			if(e) {
				callback(e);
			}
			else self.flushWal(callback);
		});

	},
	
	flushWal: function(callback) {
		var self = this;
		//flush cache
		if(self.flushWalMinutes <= 0 || (((new Date))-self.lastFlush) > self.flushWalMinutes * 60 * 1000) {
			self.lastFlush = new Date();
			self.db.run("PRAGMA wal_checkpoint", (e) => { callback(e); });
		}
		else callback(null);
	},
	
	putStream: function(key, inp, callback) {
		var self = this;
		var chunks = [];
		inp.on('data', function(chunk) {
			chunks.push( chunk );
		} );
		inp.on('end', function() {
			var buf = Buffer.concat(chunks);
			self.put( key, buf, callback );
		} );
	},
	
	head: function(key, callback) {
		// head couchbase value given key
		var self = this;
		key = self.prepKey(key);
		var stmt_head = self.db.prepare("SELECT lastmod, coalesce(size,length(val)) len FROM kv WHERE key=?");
		stmt_head.get(key, (err, data) => {
			stmt_head.finalize();
			if(!data) {
				var err = new Error("Failed to head key: " + key + ": Not found");
				err.code = "NoSuchKey";
				callback( err, null );
			}
			else {
				callback( null, { mod: Math.floor(data.lastmod / 1000), len: data.len }  );
			}
		});
	},
	
	get: function(key, callback) {
		// fetch Couchbase value given key
		var self = this;
		//this.logDebug(9, "Fetching Object: " + key);
		key = this.prepKey(key);
		var stmt = self.db.prepare("SELECT val FROM kv WHERE key=?", key)
		stmt.get(key, (err, data) => {
			stmt.finalize()
			if(!data) {
				var err = new Error("Failed to head key: " + key + ": Not found");
				err.code = "NoSuchKey";
				callback( err, null );
			}
			else {
				//this.logDebug(9, "Fetching Object: " + key + " val=" + data.val);
				var val = data.val;
				//{"type":"Buffer","data":[71,1]}
				if(!self.storage.isBinaryKey(key)) val = JSON.parse(val);
				callback( null, val);
			}
			
		});
	},
	
	getStream: function(key, callback) {
		// get readable stream to record value given key
		var self = this;
		this.get( key, function(err, buf) {
			if (err) {
				// an actual error
				err.message = "Failed to fetch key: " + key + ": " + err;
				self.logError('redis', ''+err);
				return callback(err);
			}
			else if (!buf) {
				// record not found
				var err = new Error("Failed to fetch key: " + key + ": Not found");
				err.code = "NoSuchKey";
				return callback( err, null );
			}
			var stream = new BufferStream(buf);
			callback(null, stream);
		} );
	},
	
	delete: function(key, callback) {
		var self = this;
		this.logDebug(9, "Deleting Object: " + key);
		key = this.prepKey(key);
		
		var stmt_head = self.db.prepare("SELECT lastmod, coalesce(size,length(val)) len FROM kv WHERE key=?");
		var data = stmt_head.get(key, (err, data) =>{
			stmt_head.finalize();
			if(!data) {
				err = new Error("Failed to delete key: " + key + ": Not found");
				err.code = "NoSuchKey";
				callback(err);
			}
			else {
				var stmt_delete = self.db.prepare("DELETE FROM kv WHERE key=?");
				stmt_delete.run(key, (e2, d2) => {
					stmt_delete.finalize();
					self.flushWal(callback);
				});
			
			}
		});
	},

	runMaintenance: function(callback) {
		// run daily maintenance
		callback();
	},
	
	shutdown: function(callback) {
		// shutdown storage
		this.logDebug(2, "Closing database");
		this.db.close()
		callback();
	}
	
});

var util = require('util');
var stream = require('stream');

var BufferStream = function (object, options) {
	if (object instanceof Buffer || typeof object === 'string') {
		options = options || {};
		stream.Readable.call(this, {
			highWaterMark: options.highWaterMark,
			encoding: options.encoding
		});
	} else {
		stream.Readable.call(this, { objectMode: true });
	}
	this._object = object;
};

util.inherits(BufferStream, stream.Readable);

BufferStream.prototype._read = function () {
	this.push(this._object);
	this._object = null;
};